from metadata import dataset as ds
from app_calendar import eff_date as ed
from metadata import workflow as iw
from metadata import ingestion_task as it

from ingest_app.settings import ConfigParms as sc
from ingest_app.ingest_spark import loader as sl

# Replace this with a API call in test/prod env
from dr_app import dr_app_core as drc
from dq_app import dq_app_core as dqc
from dqml_app import dqml_app_core as dqmlc
from dp_app import dp_app_core as dpc

import logging


def run_ingestion_task(ingestion_task_id: str, cycle_date: str) -> None:
    # Simulate getting the ingestion task metadata from API
    logging.info("Get ingestion task metadata")
    ingestion_task = it.IngestionTask.from_json(ingestion_task_id=ingestion_task_id)

    # Simulate getting the source dataset metadata from API
    logging.info("Get source dataset metadata")
    src_dataset = []
    if ingestion_task.ingestion_pattern.source_type == ds.DatasetKind.LOCAL_DELIM_FILE:
        src_dataset = ds.LocalDelimFileDataset.from_json(
            dataset_id=ingestion_task.source_dataset_id
        )

    logging.info("Get target dataset metadata")
    tgt_dataset = []
    if ingestion_task.ingestion_pattern.target_type == ds.DatasetKind.SPARK_TABLE:
        tgt_dataset = ds.SparkTableDataset.from_json(
            dataset_id=ingestion_task.target_dataset_id
        )

    # Get current effective date
    cur_eff_date = ed.get_cur_eff_date(
        schedule_id=src_dataset.schedule_id, cycle_date=cycle_date
    )

    cur_eff_date_yyyymmdd = ed.fmt_date_str_as_yyyymmdd(cur_eff_date)

    # Read the source data file
    source_file_path = sc.resolve_app_path(
        src_dataset.resolve_file_path(cur_eff_date_yyyymmdd)
    )

    qual_target_table_name = tgt_dataset.get_qualified_table_name()
    target_database_name = tgt_dataset.database_name
    partition_keys = tgt_dataset.partition_keys
    str_schema = get_dataset_schema(dataset_id=ingestion_task.source_dataset_id)
    load_type = ingestion_task.ingestion_pattern.load_type

    # Load the file
    records = sl.load_file_to_table(
        source_file_path=source_file_path,
        qual_target_table_name=qual_target_table_name,
        target_database_name=target_database_name,
        partition_keys=partition_keys,
        cur_eff_date=cur_eff_date,
        str_schema=str_schema,
        load_type=load_type,
    )

    logging.info("%d records are loaded into %s.", records, qual_target_table_name)


def run_data_quality_task(required_parameters: dict, cycle_date: str) -> None:
    dataset_id = required_parameters["dataset_id"]
    logging.info("Start applying data quality rules on the dataset %s", dataset_id)
    dq_check_results = dqc.apply_dq_rules(dataset_id=dataset_id, cycle_date=cycle_date)

    logging.info(
        "Finished applying data quality rules on the dataset %s",
        dataset_id,
    )

    logging.info("Data quality check results for dataset %s", dataset_id)
    logging.info(dq_check_results)


def run_data_quality_ml_task(required_parameters: dict, cycle_date: str) -> None:
    dataset_id = required_parameters["dataset_id"]
    logging.info("Started detecting anomalies in the dataset %s", dataset_id)
    column_scores = dqmlc.detect_anomalies(dataset_id=dataset_id, cycle_date=cycle_date)

    logging.info("Column/Feature scores for dataset %s", dataset_id)
    logging.info(column_scores)

    logging.info("Finished detecting anomalies in the dataset %s", dataset_id)


def run_data_profile_task(required_parameters: dict, cycle_date: str) -> None:
    dataset_id = required_parameters["dataset_id"]
    logging.info("Start profiling the dataset %s", dataset_id)
    dp_results = dpc.apply_ner_model(dataset_id=dataset_id, cycle_date=cycle_date)

    logging.info("Data profile results for dataset %s", dataset_id)
    logging.info(dp_results)

    logging.info("Finished profiling the dataset %s", dataset_id)


def run_data_recon_task(required_parameters: dict, cycle_date: str) -> None:
    dataset_id = required_parameters["dataset_id"]
    logging.info(
        "Start applying data reconciliation rules on the dataset %s", dataset_id
    )
    dr_check_results = drc.apply_dr_rules(dataset_id=dataset_id, cycle_date=cycle_date)

    logging.info(
        "Finished applying data reconciliation rules on the dataset %s",
        dataset_id,
    )

    logging.info("Data reconciliation check results for dataset %s", dataset_id)
    logging.info(dr_check_results)


def run_pre_ingestion_tasks(tasks: list[iw.ManagementTask], cycle_date: str) -> None:
    for task in tasks:
        if task.name == "data quality":
            run_data_quality_task(
                required_parameters=task.required_parameters, cycle_date=cycle_date
            )
        elif task.name == "data quality ml":
            run_data_quality_ml_task(
                required_parameters=task.required_parameters, cycle_date=cycle_date
            )
        if task.name == "data profile":
            run_data_profile_task(
                required_parameters=task.required_parameters, cycle_date=cycle_date
            )


def run_post_ingestion_tasks(tasks: list[iw.ManagementTask], cycle_date: str) -> None:
    for task in tasks:
        if task.name == "data reconciliation":
            run_data_recon_task(
                required_parameters=task.required_parameters, cycle_date=cycle_date
            )


def run_ingestion_workflow(ingestion_workflow_id: str, cycle_date: str) -> None:

    # Simulate getting the ingestion workflow metadata from API
    logging.info("Get ingestion workflow metadata")
    ingestion_workflow = iw.IngestionWorkflow.from_json(
        workflow_id=ingestion_workflow_id, workflow_kind="ingestion"
    )

    # Run pre-ingestion tasks
    logging.info("Running the pre-ingestion tasks.")
    run_pre_ingestion_tasks(tasks=ingestion_workflow.pre_tasks, cycle_date=cycle_date)

    # Run ingestion task
    logging.info("Running the ingestion task %s.", ingestion_workflow.ingestion_task_id)
    run_ingestion_task(
        ingestion_task_id=ingestion_workflow.ingestion_task_id,
        cycle_date=cycle_date,
    )

    # Run post-ingestion tasks
    logging.info("Running the post-ingestion tasks.")
    run_post_ingestion_tasks(tasks=ingestion_workflow.post_tasks, cycle_date=cycle_date)


def get_dataset_schema(dataset_id: str) -> str:
    # Specify the str schema in the format source column name, target column name, data type
    # Capture this as part of dataset metadata.
    # This is not needed if the spark tables are defined up front (as is the case in production).
    str_schemas = {
        "1": [
            ("effective_date", "effective_date", "string"),
            ("asset_id", "asset_id", "string"),
            ("asset_type", "asset_type", "string"),
            ("asset_name", "asset_name", "string"),
        ],
        "2": [
            ("effective_date", "effective_date", "string"),
            ("account_id", "account_id", "string"),
            ("asset_id", "asset_id", "string"),
            ("asset_value", "asset_value", "decimal(25,2)"),
        ],
        "3": [
            ("effective_date", "effective_date", "string"),
            ("first_name", "first_name", "string"),
            ("last_name", "last_name", "string"),
            ("full_name", "full_name", "string"),
            ("ssn", "ssn", "string"),
            ("dob", "dob", "string"),
            ("street_addr1", "street_addr1", "string"),
            ("street_addr2", "street_addr2", "string"),
            ("city", "city", "string"),
            ("state", "state", "string"),
            ("country", "country", "string"),
        ],
    }

    try:
        if dataset_id in str_schemas:
            str_schema_for_dataset = str_schemas[dataset_id]
        else:
            raise ValueError("Schema is not defined for the dataset.")
    except ValueError as error:
        logging.error(error)
        raise

    return str_schema_for_dataset
