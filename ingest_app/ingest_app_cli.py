import logging
import os

import click
from ingest_app import settings as sc
from ingest_app import ingest_app_core as dic
from utils import logger as ufl


@click.command()
# @click.argument('ingest_workflow_id', required=1)
@click.option(
    "--ingestion_workflow_id",
    type=str,
    default="0",
    help="Ingestion workflow id",
    required=True,
)
@click.option("--env", type=str, default="dev", help="Environment")
@click.option("--cycle_date", type=str, default="", help="Cycle date")
def run_ingestion_workflow(ingestion_workflow_id: str, env: str, cycle_date: str):
    """
    Run the ingestion workflow.
    """

    cfg = sc.load_config(env)
    sc.set_config(cfg)

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    ufl.config_logger(log_file_path_name=f"{sc.log_file_path}/{script_name}.log")
    logging.info("Configs are set")

    logging.info("Running the ingestion workflow %s", ingestion_workflow_id)
    records_count = dic.run_ingestion_workflow(
        ingestion_workflow_id=ingestion_workflow_id, cycle_date=cycle_date
    )

    logging.info("Records loaded by the ingestion workflow %s", ingestion_workflow_id)
    logging.info(records_count)

    logging.info("Finished running the ingestion workflow %s", ingestion_workflow_id)


# Create command group
@click.group()
def cli():
    pass


# Add sub command to group
cli.add_command(run_ingestion_workflow)


def main():
    cli()


if __name__ == "__main__":
    main()
