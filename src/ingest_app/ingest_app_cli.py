import logging
import os

import click
from ingest_app.settings import ConfigParms as sc
from ingest_app import settings as scg

# Needed to pass the cfg from main app to sub app
from dr_app.settings import ConfigParms as dr_sc
from dr_app import settings as dr_scg
from dq_app.settings import ConfigParms as dq_sc
from dq_app import settings as dq_scg
from dqml_app.settings import ConfigParms as dqml_sc
from dqml_app import settings as dqml_scg
from dp_app.settings import ConfigParms as dp_sc
from dp_app import settings as dp_scg

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

    sc.load_config(env)
    # Override sub app config with main app cfg
    dr_scg.APP_ROOT_DIR = scg.APP_ROOT_DIR
    dr_sc.load_config(env)
    dq_scg.APP_ROOT_DIR = scg.APP_ROOT_DIR
    dq_sc.load_config(env)
    dqml_scg.APP_ROOT_DIR = scg.APP_ROOT_DIR
    dqml_sc.load_config(env)
    dp_scg.APP_ROOT_DIR = scg.APP_ROOT_DIR
    dp_sc.load_config(env)

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
