from metadata import dataset as ds
from metadata import data_source as dsrc
from app_calendar import eff_date as ed
from metadata import workflow as iw
from metadata import integration_task as it
from metadata import dataset_schema as dh
from gov import gov_tasks as dg

from config.settings import ConfigParms as sc

import logging
import os

from utils import spark_io as ufs

import json


def run_ingestion_workflow(ingestion_workflow_id: str, cycle_date: str) -> list[dict]:
    # Simulate getting the cycle date from API
    # Run this from the parent app
    if not cycle_date:
        cycle_date = ed.get_cur_cycle_date()

    # Simulate getting the ingestion workflow metadata from API
    logging.info("Get ingestion workflow metadata")
    ingestion_workflow = iw.IngestionWorkflow.from_json(
        workflow_id=ingestion_workflow_id
    )

    # Run pre-ingestion tasks
    logging.info("Running the pre-ingestion tasks.")
    pre_ingestion_task_results = run_pre_ingestion_tasks(
        tasks=ingestion_workflow.pre_tasks, cycle_date=cycle_date
    )

    # Run ingestion task
    logging.info("Running the ingestion task %s.", ingestion_workflow.ingestion_task_id)
    ingestion_task_status_code, ingestion_task_results = run_ingestion_task(
        ingestion_task_id=ingestion_workflow.ingestion_task_id,
        cycle_date=cycle_date,
    )

    # Run post-ingestion tasks
    if not ingestion_task_status_code:
        logging.info("Running the post-ingestion tasks.")
        post_ingestion_task_results = run_post_ingestion_tasks(
            tasks=ingestion_workflow.post_tasks, cycle_date=cycle_date
        )

    # Data lineage is required for all workflows
    dl_results = dg.run_data_lineage_task(
        workflow_id=ingestion_workflow_id, cycle_date=cycle_date
    )

    results = pre_ingestion_task_results
    if not ingestion_task_status_code:
        results.append(ingestion_task_results)

    if not ingestion_task_status_code:
        results.append(post_ingestion_task_results)

    results.append(dl_results)

    logging.debug(results)
    return results


def run_ingestion_task(ingestion_task_id: str, cycle_date: str) -> None:
    # Simulate getting the ingestion task metadata from API
    logging.info("Get ingestion task metadata")
    ingestion_task = it.IngestionTask.from_json(task_id=ingestion_task_id)

    # Simulate getting the source dataset metadata from API
    logging.info("Get source dataset metadata")
    source_dataset = ds.get_dataset_from_json(
        dataset_id=ingestion_task.source_dataset_id
    )

    # Simulate getting the data source metadata from API
    data_source = dsrc.get_data_source_from_json(
        data_source_id=source_dataset.data_source_id
    )

    # Get current effective date
    cur_eff_date = ed.get_cur_eff_date(
        schedule_id=source_dataset.schedule_id, cycle_date=cycle_date
    )

    cur_eff_date_yyyymmdd = ed.fmt_date_str_as_yyyymmdd(cur_eff_date)

    if source_dataset.dataset_type == ds.DatasetType.LOCAL_DELIM_FILE:
        source_file_path = sc.resolve_app_path(
            source_dataset.resolve_file_path(
                date_str=cur_eff_date_yyyymmdd,
                data_source_user=data_source.data_source_user,
            )
        )
    elif source_dataset.dataset_type == ds.DatasetType.AWS_S3_DELIM_FILE:
        source_file_path = sc.resolve_app_path(
            source_dataset.resolve_file_uri(
                date_str=cur_eff_date_yyyymmdd,
                data_source_user=data_source.data_source_user,
            )
        )
    else:
        raise RuntimeError("Source dataset type is not expected.")

    # Simulate getting the target dataset metadata from API
    logging.info("Get target dataset metadata")
    target_dataset = ds.get_dataset_from_json(
        dataset_id=ingestion_task.target_dataset_id
    )

    # Simulate getting the data source metadata from API
    data_source = dsrc.get_data_source_from_json(
        data_source_id=target_dataset.data_source_id
    )

    # Simulate getting the ingestion task metadata from API
    logging.info("Get target dataset schema metadata")
    target_dataset_schema = dh.DatasetSchema.from_json(
        dataset_id=ingestion_task.target_dataset_id
    )

    # Get current effective date
    cur_eff_date = ed.get_cur_eff_date(
        schedule_id=target_dataset.schedule_id, cycle_date=cycle_date
    )
    cur_eff_date_yyyymmdd = ed.fmt_date_str_as_yyyymmdd(cur_eff_date)

    qual_target_table_name = target_dataset.get_qualified_table_name()
    target_database_name = target_dataset.database_name
    partition_keys = json.dumps(target_dataset.partition_keys).replace(
        " ", ""
    )  # convert to string
    target_table_schema = json.dumps(target_dataset_schema.schema).replace(
        " ", ""
    )  # convert to string
    # print(target_table_schema)
    load_type = ingestion_task.ingestion_pattern.load_type
    debug = "n"

    # Load data
    logging.info("Loading data")

    spark_submit_command_str = f"""
    {os.environ['SPARK_HOME']}/bin/spark-submit \
    --master {sc.spark_master_uri} \
    --deploy-mode {sc.spark_deploy_mode} \
    --num-executors 1 \
    --executor-memory=2g \
    --executor-cores 1 \
    {sc.app_root_dir}/src/pyspark_apps/loader.py \
    --warehouse_path={sc.spark_warehouse_path} \
    --spark_master_uri={sc.spark_master_uri} \
    --spark_history_log_dir={sc.spark_history_log_dir} \
    --spark_local_dir={sc.spark_local_dir} \
    --postgres_uri={sc.postgres_uri} \
    --source_file_path={source_file_path} \
    --qual_target_table_name={qual_target_table_name} \
    --target_database_name={target_database_name} \
    --partition_keys={partition_keys} \
    --cur_eff_date={cur_eff_date} \
    --target_table_schema={target_table_schema} \
    --load_type={load_type} \
    --debug={debug}
    """
    # ufs.spark_submit_with_callback(
    #     command=spark_submit_command_str, callback=spark_submit_callback
    # )
    status_code = ufs.spark_submit_with_wait(command=spark_submit_command_str)

    ingestion_task_results = [
        {
            "ingestion task id": ingestion_task_id,
            "source dataset name": source_file_path,
            "target table name": qual_target_table_name,
            "source record count": 0,
            "target record count": 0,
        }
    ]
    return status_code, ingestion_task_results


def run_pre_ingestion_tasks(
    tasks: list[iw.ManagementTask], cycle_date: str
) -> list[dict]:
    pre_ingestion_task_results = []
    pre_ingestion_task_results = dg.run_management_tasks(
        tasks=tasks, cycle_date=cycle_date
    )
    return pre_ingestion_task_results


def run_post_ingestion_tasks(
    tasks: list[iw.ManagementTask], cycle_date: str
) -> list[dict]:
    post_ingestion_task_results = []
    post_ingestion_task_results = dg.run_management_tasks(
        tasks=tasks, cycle_date=cycle_date
    )
    return post_ingestion_task_results
