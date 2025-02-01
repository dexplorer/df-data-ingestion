from metadata import dataset as ds
from metadata import ingestion_workflow as iw
from metadata import ingestion_task as it
from utils import file_io as uff

from ingest_app import settings as sc
from ingest_app.ingest_spark import loader as sl

import logging


def ingest_file(ingestion_workflow_id) -> list:

    # Simulate getting the ingestion workflow metadata from API
    logging.info("Get ingestion workflow metadata")
    ingestion_workflow = iw.IngestionWorkflow.from_json(
        ingestion_workflow_id=ingestion_workflow_id
    )

    # Simulate getting the ingestion task metadata from API
    logging.info("Get ingestion task metadata")
    ingestion_task = it.IngestionTask.from_json(
        ingestion_task_id=ingestion_workflow.ingestion_task_id
    )

    # Simulate getting the source dataset metadata from API
    logging.info("Get source dataset metadata")
    if ingestion_task.ingestion_pattern.source_type == "delimited file":
        src_dataset = ds.LocalDelimFileDataset.from_json(
            dataset_id=ingestion_task.source_dataset_id
        )

    if ingestion_task.ingestion_pattern.target_type == "spark table":
        tgt_dataset = ds.SparkTableDataset.from_json(
            dataset_id=ingestion_task.target_dataset_id
        )

    # Get current effective date
    cur_eff_date = ed.get_cur_eff_date(schedule_id=dataset.schedule_id)
    cur_eff_date_yyyymmdd = ed.fmt_date_str_as_yyyymmdd(cur_eff_date)

    # Read the source data file
    source_file_path = sc.resolve_app_path(
        src_dataset.resolve_file_path(cur_eff_date_yyyymmdd)
    )

    # source_file_path = "/workspaces/df-data-ingestion/ingest_app/data/acct_positions_20241226.csv"
    target_file_path = "/workspaces/df-data-ingestion/ingest_app/data/output/tacct_position"

    # Load the file
    records, columns = sl.load_file_to_table(source_file_path, target_file_path)

    print(f"{records} records are loaded.")
    logging.info("%d records are loaded.", records)
    return records
