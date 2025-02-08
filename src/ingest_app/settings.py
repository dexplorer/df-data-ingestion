import confuse

import logging

APP_ROOT_DIR = "/workspaces/df-data-ingestion"


class ConfigParms:
    config = confuse.Configuration("ingest_app", __name__)

    # Define config variables at module scope
    cfg_file_path = ""
    log_file_path = ""
    source_file_path = ""
    warehouse_path = ""

    @classmethod
    def load_config(cls, env: str):
        try:
            if env == "prod":
                cls.config.set_file(f"{APP_ROOT_DIR}/cfg/config.yaml")
            elif env == "qa":
                cls.config.set_file(f"{APP_ROOT_DIR}/cfg/config_qa.yaml")
            elif env == "dev":
                cls.config.set_file(f"{APP_ROOT_DIR}/cfg/config_dev.yaml")
            else:
                raise ValueError(
                    "Environment is invalid. Accepted values are prod / qa / dev ."
                )
        except ValueError as error:
            logging.error(error)
            raise

        cfg = cls.config["CONFIG"].get()
        logging.info(cfg)

        cls.cfg_file_path = f"{cls.resolve_app_path(cfg['cfg_file_path'])}"
        cls.log_file_path = f"{cls.resolve_app_path(cfg['log_file_path'])}"
        cls.source_file_path = f"{cls.resolve_app_path(cfg['source_file_path'])}"
        cls.warehouse_path = f"{cls.resolve_app_path(cfg['warehouse_path'])}"

    @staticmethod
    def resolve_app_path(rel_path):
        return rel_path.replace("APP_ROOT_DIR", APP_ROOT_DIR)
