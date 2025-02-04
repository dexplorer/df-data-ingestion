import confuse

import logging

config = confuse.Configuration("ingest_app", __name__)

# global APP_ROOT_DIR
APP_ROOT_DIR = "/workspaces/df-data-ingestion/ingest_app"

# Define config variables at module scope
log_file_path = ""
source_file_path = ""
warehouse_path = ""


def load_config(env):
    try:
        if env == "prod":
            config.set_file(f"{APP_ROOT_DIR}/cfg/config.yaml")
        elif env == "qa":
            config.set_file(f"{APP_ROOT_DIR}/cfg/config_qa.yaml")
        elif env == "dev":
            config.set_file(f"{APP_ROOT_DIR}/cfg/config_dev.yaml")
        else:
            raise ValueError(
                "Environment is invalid. Accepted values are prod / qa / dev ."
            )
    except ValueError as error:
        logging.error(error)
        raise

    cfg = config["CONFIG"].get()
    logging.info(cfg)

    global log_file_path
    log_file_path = f"{resolve_app_path(cfg['log_file_path'])}"

    global source_file_path
    source_file_path = f"{resolve_app_path(cfg['source_file_path'])}"

    global warehouse_path
    warehouse_path = f"{resolve_app_path(cfg['warehouse_path'])}"


def resolve_app_path(rel_path):
    return rel_path.replace("APP_ROOT_DIR", APP_ROOT_DIR)
