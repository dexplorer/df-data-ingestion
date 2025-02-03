from metadata import dataset as ds
from app_calendar import eff_date as ed
from metadata import ingestion_workflow as iw
from metadata import ingestion_task as it
from utils import file_io as uff

from ingest_app import settings as sc
from ingest_app.ingest_spark import loader as sl

# Replace this with a API call in test/prod env
from dr_app import dr_app_core as drc

import logging


def run_ingestion_task(ingestion_task_id: str, cycle_date: str) -> int:
    # Simulate getting the ingestion task metadata from API
    logging.info("Get ingestion task metadata")
    ingestion_task = it.IngestionTask.from_json(ingestion_task_id=ingestion_task_id)

    # Simulate getting the source dataset metadata from API
    logging.info("Get source dataset metadata")
    if ingestion_task.ingestion_pattern.source_type == ds.DatasetKind.LOCAL_DELIM_FILE:
        src_dataset = ds.LocalDelimFileDataset.from_json(
            dataset_id=ingestion_task.source_dataset_id
        )

    logging.info("Get target dataset metadata")
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

    # source_file_path = "/workspaces/df-data-ingestion/ingest_app/data/acct_positions_20241226.csv"
    # target_file_path = "/workspaces/df-data-ingestion/ingest_app/data/output/tacct_position"

    qual_target_table_name = tgt_dataset.get_qualified_table_name()
    target_database_name = tgt_dataset.database_name
    target_table_name = tgt_dataset.table_name
    partition_keys = tgt_dataset.partition_keys

    # Load the file
    records = sl.load_file_to_table(
        source_file_path=source_file_path,
        qual_target_table_name=qual_target_table_name,
        target_database_name=target_database_name,
        target_table_name=target_table_name,
        partition_keys=partition_keys,
        cur_eff_date=cur_eff_date,
    )

    print(f"{records} records are loaded into {qual_target_table_name}.")
    logging.info("%d records are loaded into %s.", records, qual_target_table_name)
    return records


def run_pre_ingestion_tasks():
    pass


def run_post_ingestion_tasks(tasks: list[iw.ManagementTask], cycle_date: str):
    for task in tasks:
        if task.name == "data reconciliation":
            dataset_id = task.required_parameters["dataset_id"]
            logging.info(
                "Start applying data reconciliation rules on the dataset %s", dataset_id
            )
            dr_check_results = drc.apply_dr_rules(
                dataset_id=dataset_id, cycle_date=cycle_date
            )

            logging.info(
                "Finished applying data reconciliation rules on the dataset %s",
                dataset_id,
            )

            logging.info("Data reconciliation check results for dataset %s", dataset_id)
            logging.info(dr_check_results)


def run_ingestion_workflow(ingestion_workflow_id: str, cycle_date: str) -> None:

    # Simulate getting the ingestion workflow metadata from API
    logging.info("Get ingestion workflow metadata")
    ingestion_workflow = iw.IngestionWorkflow.from_json(
        ingestion_workflow_id=ingestion_workflow_id
    )

    # Run pre-ingestion tasks
    logging.info("Running the pre-ingestion tasks.")

    # Run ingestion task
    logging.info("Running the ingestion task.")
    records_loaded = run_ingestion_task(
        ingestion_task_id=ingestion_workflow.ingestion_task_id,
        cycle_date=cycle_date,
    )

    # Run post-ingestion tasks
    logging.info("Running the post-ingestion tasks.")
    run_post_ingestion_tasks(
        tasks=ingestion_workflow.post_ingestion_tasks, cycle_date=cycle_date
    )
