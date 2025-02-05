from ingest_app import settings as sc

# Needed to pass the cfg from main app to sub app
from dr_app import settings as dr_sc
from dq_app import settings as dq_sc
from dqml_app import settings as dqml_sc
from dp_app import settings as dp_sc

from ingest_app import ingest_app_core as dic
import logging

from fastapi import FastAPI
import uvicorn

app = FastAPI()


@app.get("/")
async def root():
    """
    Default route

    Args:
        name: The user's name.

    Returns:
        A default message.
    """

    return {"message": "Data Ingestion App"}


# @app.get("/run-ingestion/")
# async def run_ingestion(ingestion_workflow_id: str, env: str = "dev", cycle_date: str = ""):
#     return {"message": f"Parameters: ingestion_workflow_id={ingestion_workflow_id}, env={env}, cycle_date={cycle_date}"}


@app.get("/run-ingestion-workflow/")
async def run_ingestion_workflow(
    ingestion_workflow_id: str, env: str = "dev", cycle_date: str = ""
):
    """
    Runs the ingestion workflow.

    Args:
        dataset_id: Id of the dataset.
        env: Environment
        cycle_date: Cycle date

    Returns:
        Results from the data reconciliation validations.
    """

    sc.load_config(env)
    # Override sub app config with main app cfg
    dr_sc.APP_ROOT_DIR = sc.APP_ROOT_DIR
    dr_sc.load_config(env)
    dq_sc.APP_ROOT_DIR = sc.APP_ROOT_DIR
    dq_sc.load_config(env)
    dqml_sc.APP_ROOT_DIR = sc.APP_ROOT_DIR
    dqml_sc.load_config(env)
    dp_sc.APP_ROOT_DIR = sc.APP_ROOT_DIR
    dp_sc.load_config(env)

    logging.info("Configs are set")

    logging.info("Running the ingestion workflow %s", ingestion_workflow_id)
    dic.run_ingestion_workflow(
        ingestion_workflow_id=ingestion_workflow_id, cycle_date=cycle_date
    )
    logging.info("Finished running the ingestion workflow %s", ingestion_workflow_id)

    return {"return_code": 0}


if __name__ == "__main__":
    uvicorn.run(
        app,
        port=8080,
        host="0.0.0.0",
        log_config=f"{sc.APP_ROOT_DIR}/cfg/api_log.ini",
    )
