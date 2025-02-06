import os
import argparse
import logging

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

from fastapi import FastAPI
import uvicorn

app = FastAPI()


@app.get("/")
async def root():
    """
    Default route

    Args:
        none

    Returns:
        A default message.
    """

    return {"message": "Data Ingestion App"}


@app.get("/run-ingestion-workflow/")
async def run_ingestion_workflow(
    ingestion_workflow_id: str, cycle_date: str = ""
):
    """
    Runs the ingestion workflow.

    Args:
        dataset_id: Id of the dataset.
        cycle_date: Cycle date

    Returns:
        Results from the data reconciliation validations.
    """

    logging.info("Running the ingestion workflow %s", ingestion_workflow_id)
    dic.run_ingestion_workflow(
        ingestion_workflow_id=ingestion_workflow_id, cycle_date=cycle_date
    )
    logging.info("Finished running the ingestion workflow %s", ingestion_workflow_id)

    return {"return_code": 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Ingestion Application")
    parser.add_argument(
        "-e", "--env", help="Environment", const="dev", nargs="?", default="dev"
    )

    # Get the arguments
    args = vars(parser.parse_args())
    logging.info(args)
    env = args["env"]

    sc.load_config(env)
    # Override sub app config with main app cfg
    dr_scg.APP_ROOT_DIR = scg.APP_ROOT_DIR
    dr_sc.load_config(env)
    dq_scg.APP_ROOT_DIR = scg.APP_ROOT_DIR
    dq_sc.load_config(env)
    dqml_sc.APP_ROOT_DIR = scg.APP_ROOT_DIR
    dqml_sc.load_config(env)
    dp_scg.APP_ROOT_DIR = scg.APP_ROOT_DIR
    dp_sc.load_config(env)

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    ufl.config_logger(log_file_path_name=f"{sc.log_file_path}/{script_name}.log")
    logging.info("Configs are set")

    logging.info("Starting the API service")

    uvicorn.run(
        app,
        port=8080,
        host="0.0.0.0",
        log_config=f"{sc.cfg_file_path}/api_log.ini",
    )

    logging.info("Stopping the API service")
