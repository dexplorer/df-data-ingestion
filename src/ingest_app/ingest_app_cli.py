import logging
import os

import click
from dotenv import load_dotenv
from config.settings import ConfigParms as sc
from ingest_app import ingest_app_core as dic
from utils import logger as ufl


# Create command group
@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--ingestion_workflow_id",
    type=str,
    help="Ingestion workflow id",
    required=True,
)
@click.option("--cycle_date", type=str, default="", help="Cycle date")
def run_ingestion_workflow(ingestion_workflow_id: str, cycle_date: str):
    """
    Run the ingestion workflow.
    """

    logging.info("Start running the ingestion workflow %s", ingestion_workflow_id)
    dic.run_ingestion_workflow(
        ingestion_workflow_id=ingestion_workflow_id, cycle_date=cycle_date
    )
    logging.info("Finished running the ingestion workflow %s", ingestion_workflow_id)


def main():
    # Load the environment variables from .env file
    load_dotenv()

    # Fail if env variable is not set
    sc.env = os.environ["ENV"]
    sc.app_root_dir = os.environ["APP_ROOT_DIR"]
    sc.load_config()

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    ufl.config_logger(log_file_path_name=f"{sc.log_file_path}/{script_name}.log")
    logging.info("Configs are set")
    logging.info(os.environ)
    logging.info(sc.config)
    logging.info(vars(sc))

    cli()


if __name__ == "__main__":
    main()
