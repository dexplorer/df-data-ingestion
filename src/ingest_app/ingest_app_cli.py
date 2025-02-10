import logging
import os

import click

from config.settings import ConfigParms as sc
from config import settings as scg

from ingest_app import ingest_app_core as dic
from utils import logger as ufl

#
APP_ROOT_DIR = "/workspaces/df-data-ingestion"


# Create command group
@click.group()
def cli():
    pass


@cli.command()
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

    scg.APP_ROOT_DIR = APP_ROOT_DIR
    sc.load_config(env)

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    ufl.config_logger(log_file_path_name=f"{sc.log_file_path}/{script_name}.log")
    logging.info("Configs are set")

    logging.info("Running the ingestion workflow %s", ingestion_workflow_id)
    dic.run_ingestion_workflow(
        ingestion_workflow_id=ingestion_workflow_id, cycle_date=cycle_date
    )
    logging.info("Finished running the ingestion workflow %s", ingestion_workflow_id)


def main():
    cli()


if __name__ == "__main__":
    main()
