import argparse
import logging
import os

import uvicorn
from fastapi import FastAPI

from config.settings import ConfigParms as sc
from ingest_app import ingest_app_core as dic
from utils import logger as ufl

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
async def run_ingestion_workflow(ingestion_workflow_id: str, cycle_date: str = ""):
    """
    Runs the ingestion workflow.

    Args:
        dataset_id: Id of the dataset.
        cycle_date: Cycle date

    Returns:
        Results from the data reconciliation validations.
    """

    logging.info("Running the ingestion workflow %s", ingestion_workflow_id)
    results = dic.run_ingestion_workflow(
        ingestion_workflow_id=ingestion_workflow_id, cycle_date=cycle_date
    )
    logging.info("Finished running the ingestion workflow %s", ingestion_workflow_id)

    return {"results": results}


def main():
    parser = argparse.ArgumentParser(description="Data Ingestion Application")
    parser.add_argument(
        "--app_host_pattern",
        help="Environment where the application is hosted.",
        nargs=None,  # 1 argument values
        required=True,
    )
    parser.add_argument(
        "--debug",
        help="Set the logging level to DEBUG",
        nargs="?",  # 0-or-1 argument values
        const="y",  # default when the argument is provided with no value
        default="n",  # default when the argument is not provided
        required=False,
    )

    # Get the arguments
    args = vars(parser.parse_args())
    app_host_pattern = args["app_host_pattern"]
    debug = args["debug"]
    if debug == "y":
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    # Set root logger level
    root_logger = logging.getLogger()
    root_logger.setLevel(level=log_level)

    # Set env anf cfg variables
    sc.load_config(app_host_pattern)

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    ufl.config_multi_platform_logger(
        log_level=log_level,
        handlers=sc.log_handlers,
        log_file_path_name=f"{sc.app_log_path}/{script_name}.log",
    )
    logging.info("Configs are set")
    logging.info(os.environ)
    logging.info(sc.config)
    logging.info(vars(sc))

    logging.info("Starting the API service")

    uvicorn.run(
        app,
        port=int(os.environ["API_PORT"]),
        host=os.environ["API_HOST"],
        log_config=None,
    )

    logging.info("Stopping the API service")


if __name__ == "__main__":
    main()
