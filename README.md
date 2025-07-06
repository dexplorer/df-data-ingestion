# df-data-ingestion

This application runs a data ingestion workflow to ingest a delimited file into a spark table. 

It runs the configured pre-ingestion (data quality checks, data profiling) and post-ingestion (data reconciliation) tasks as part of the workflow.

Also, it runs the data lineage task to capture the relationships. This is not optional.

Application can be invoked using CLI or REST API end points. This allows the app to be integrated into a larger data ingestion / distribution framework.

## Sub Modules
The data distribution service leverages the following services to perform the tasks in the data distribution pipeline.

[Metadata Management Service](https://github.com/dexplorer/df-metadata)

[Application Calendar Service](https://github.com/dexplorer/df-app-calendar)

[Data Quality Service](https://github.com/dexplorer/df-data-quality)

[Data Quality ML Service](https://github.com/dexplorer/df-data-quality-ml)

[Data Profiling Service](https://github.com/dexplorer/df-data-profile)

[Data Reconciliation Service](https://github.com/dexplorer/df-data-recon)

[Data Lineage Service](https://github.com/dexplorer/df-data-lineage)

## Data Flow

![Data Ingestion Pipeline](docs/df-data-ingestion.png?raw=true "Data Ingestion Pipeline")

### Define the Environment Variables

Update one of the following .env files which is appropriate for the application hosting pattern.

```
app_env.on_prem_vm_native.dev.env
app_env.aws_ec2_native.dev.env
app_env.aws_ec2_container.dev.env
app_env.aws_ecs_container.dev.env
```

### Install

- **Install via Makefile and pip**:
  ```
    make install-dev
  ```


### Start the Spark Standalone Cluster

In df-spark project,
```sh
APP_INFRA_USER_NAME="ec2-user" APP_INFRA_USER_GROUP_NAME="ec2-user" docker-compose up --build
```

### Start the Data Management Services

In df-data-governance project,
```sh
docker-compose --project-name=df-spark up
```

### Start the Data Management Services Individually

##### Data Lineage Service
  ```sh
    docker run \
    --mount=type=bind,src=/nas,dst=/nas \
    -p 9091:9090 \
    --rm -it df-data-lineage:latest \
    dl-app-api --app_host_pattern "aws_ec2_container"
  ```

##### Data Profile Service
  ```sh
    docker run \
    --mount=type=bind,src=/nas,dst=/nas \
    -p 9092:9090 \
    --rm -it df-data-profile:latest \
    dp-app-api --app_host_pattern "aws_ec2_container"
  ```

##### Data Quality Service
  ```sh
    docker run \
    --mount=type=bind,src=/nas,dst=/nas \
    -p 9093:9090 \
    --rm -it df-data-quality:latest \
    dq-app-api --app_host_pattern "aws_ec2_container"
  ```

##### Data Quality ML Service
  ```sh
    docker run \
    --mount=type=bind,src=/nas,dst=/nas \
    -p 9094:9090 \
    --rm -it df-data-quality-ml:latest \
    dqml-app-api --app_host_pattern "aws_ec2_container"
  ```

##### Data Reconciliation Service
  ```sh
    docker run \
    --mount=type=bind,src=/nas,dst=/nas \
    -p 9095:9090 \
    --rm -it df-data-recon:latest \
    dr-app-api --app_host_pattern "aws_ec2_container"
  ```

### Usage Examples

- **Run a ingestion workflow via CLI**:
  ```sh
    ingest-app-cli --app_host_pattern "aws_ec2_native" run-ingestion-workflow --ingestion_workflow_id "workflow_1"
    ingest-app-cli --app_host_pattern "aws_ec2_native" run-ingestion-workflow --ingestion_workflow_id "workflow_2"
    ingest-app-cli --app_host_pattern "aws_ec2_native" run-ingestion-workflow --ingestion_workflow_id "workflow_3"

    ingest-app-cli --app_host_pattern "aws_ec2_native" run-ingestion-workflow --ingestion_workflow_id "workflow_101"
    ingest-app-cli --app_host_pattern "aws_ec2_native" run-ingestion-workflow --ingestion_workflow_id "workflow_102"
    ingest-app-cli --app_host_pattern "aws_ec2_native" run-ingestion-workflow --ingestion_workflow_id "workflow_103"
  ```

- **Run a ingestion workflow via CLI with cycle date override**:
  ```sh
    ingest-app-cli --app_host_pattern "aws_ec2_native" run-ingestion-workflow --ingestion_workflow_id "workflow_1" --cycle_date "2024-12-26"
    ingest-app-cli --app_host_pattern "aws_ec2_native" run-ingestion-workflow --ingestion_workflow_id "workflow_2" --cycle_date "2024-12-26"
    ingest-app-cli --app_host_pattern "aws_ec2_native" run-ingestion-workflow --ingestion_workflow_id "workflow_3" --cycle_date "2024-12-26"
  ```

- **Run a Ingestion Workflow via API**:
  ##### Start the API server
  ```sh
    ingest-app-api --app_host_pattern "aws_ec2_native"
  ```
  ##### Invoke the API endpoint
  ```sh
    https://<host name with port number>/run-ingestion-workflow/?ingestion_workflow_id=<value>
    https://<host name with port number>/run-ingestion-workflow/?ingestion_workflow_id=<value>&cycle_date=<value>

    /run-ingestion-workflow/?ingestion_workflow_id=workflow_1
    /run-ingestion-workflow/?ingestion_workflow_id=workflow_2
    /run-ingestion-workflow/?ingestion_workflow_id=workflow_3
    /run-ingestion-workflow/?ingestion_workflow_id=workflow_1&cycle_date=2024-12-26
    /run-ingestion-workflow/?ingestion_workflow_id=workflow_2&cycle_date=2024-12-26
    /run-ingestion-workflow/?ingestion_workflow_id=workflow_3&cycle_date=2024-12-26
  ```
  ##### Invoke the API from Swagger Docs interface
  ```sh
    https://<host name with port number>/docs
  ```

### Sample Input

  ##### Dataset (acct_positions_20241226.csv)
```
effective_date,account_id,asset_id,asset_value
2024-12-26,ACC1,1,-35000
2024-12-26,ACC1,2,-15000
2024-12-26,ACC2,2,10000
2024-12-26,ACC4,1,-5000
2024-12-26,ACC5,1,-5000
2024-12-26,ACC6,1,-5000
2024-12-26,ACC7,1,-5000
2024-12-26,ACC8,1,-5000
2024-12-26,ACC9,1,-5000
```

### API Data (simulated)
These are metadata that would be captured via the Metadata Management UI and stored in a database.

  ##### datasets 
```
{
  "datasets": [
    {
      "dataset_id": "dataset_1",
      "dataset_type": "local delim file",
      "file_delim": ",",
      "file_path": "APP_DATA_IN_DIR/assets_yyyymmdd.csv",
      "schedule_id": "schedule_2",
      "data_source_id": "data_source_1",
      "recon_file_delim": "|",
      "recon_file_path": "APP_DATA_IN_DIR/assets_yyyymmdd.recon"
    },
    {
      "dataset_id": "dataset_2",
      "dataset_type": "local delim file",
      "file_delim": ",",
      "file_path": "APP_DATA_IN_DIR/acct_positions_yyyymmdd.csv",
      "schedule_id": "schedule_2",
      "data_source_id": "data_source_2",
      "recon_file_delim": "|",
      "recon_file_path": "APP_DATA_IN_DIR/acct_positions_yyyymmdd.recon"
    },
    {
      "dataset_id": "dataset_3",
      "dataset_type": "local delim file",
      "file_delim": ",",
      "file_path": "APP_DATA_IN_DIR/customers_yyyymmdd.csv",
      "schedule_id": "schedule_2",
      "data_source_id": "data_source_3",
      "recon_file_delim": "|",
      "recon_file_path": "APP_DATA_IN_DIR/customers_yyyymmdd.recon"
    },
    {
      "dataset_id": "dataset_4",
      "dataset_type": "spark sql file",
      "schedule_id": "schedule_2",
      "data_source_id": "data_source_4",
      "sql_file_path": "APP_SQL_SCRIPT_DIR/ext_asset_value_agg.sql"
    },
    {
      "dataset_id": "dataset_11",
      "dataset_type": "spark table",
      "schedule_id": "schedule_2",
      "data_source_id": "data_source_4",
      "database_name": "dl_asset_mgmt",
      "table_name": "tasset",
      "partition_keys": [],
      "recon_file_delim": "|",
      "recon_file_path": "APP_DATA_IN_DIR/assets_yyyymmdd.recon"
    },
    {
      "dataset_id": "dataset_12",
      "dataset_type": "spark table",
      "schedule_id": "schedule_2",
      "data_source_id": "data_source_4",
      "database_name": "dl_asset_mgmt",
      "table_name": "tacct_pos",
      "partition_keys": [
        "effective_date"
      ],
      "recon_file_delim": "|",
      "recon_file_path": "APP_DATA_IN_DIR/acct_positions_yyyymmdd.recon"
    },
    {
      "dataset_id": "dataset_13",
      "dataset_type": "spark table",
      "schedule_id": "schedule_2",
      "data_source_id": "data_source_4",
      "database_name": "dl_asset_mgmt",
      "table_name": "tcustomer",
      "partition_keys": [
        "effective_date"
      ],
      "recon_file_delim": "|",
      "recon_file_path": "APP_DATA_IN_DIR/customers_yyyymmdd.recon"
    },
    {
      "dataset_id": "dataset_14",
      "dataset_type": "local delim file",
      "file_delim": "|",
      "file_path": "APP_DATA_OUT_DIR/asset_value_agg_yyyymmdd.dat",
      "schedule_id": "schedule_2",
      "data_source_id": "data_source_4",
      "recon_file_delim": null,
      "recon_file_path": null
    } 
  ]
}

```

  ##### Ingestion Workflows 
```
{
    "workflows": [
      {
        "workflow_id": "workflow_1",
        "workflow_type": "ingestion", 
        "ingestion_task_id": "integration_task_1",
        "pre_tasks": [
          {
            "name": "data quality",
            "required_parameters": {
              "dataset_id": "dataset_1"
            }
          },
          {
            "name": "data profile",
            "required_parameters": {
              "dataset_id": "dataset_1"
            }
          }
        ],
        "post_tasks": [
        ]
      },
      {
        "workflow_id": "workflow_2",
        "workflow_type": "ingestion", 
        "ingestion_task_id": "integration_task_2",
        "pre_tasks": [
          {
            "name": "data quality",
            "required_parameters": {
              "dataset_id": "dataset_2"
            }
          },
          {
            "name": "data quality ml",
            "required_parameters": {
              "dataset_id": "dataset_2"
            }
          },
          {
            "name": "data profile",
            "required_parameters": {
              "dataset_id": "dataset_2"
            }
          }
        ],
        "post_tasks": [
          {
            "name": "data reconciliation",
            "required_parameters": {
              "dataset_id": "dataset_12"
            }
          }
        ]
      },
      {
        "workflow_id": "workflow_3",
        "workflow_type": "ingestion", 
        "ingestion_task_id": "integration_task_3",
        "pre_tasks": [
          {
            "name": "data quality",
            "required_parameters": {
              "dataset_id": "dataset_3"
            }
          },
          {
            "name": "data profile",
            "required_parameters": {
              "dataset_id": "dataset_3"
            }
          }
        ],
        "post_tasks": [
        ]
      },
      {
        "workflow_id": "workflow_11",
        "workflow_type": "distribution", 
        "distribution_task_id": "integration_task_11",
        "pre_tasks": [
        ],
        "post_tasks": [
          {
            "name": "data quality",
            "required_parameters": {
              "dataset_id": "dataset_14"
            }
          },
          {
            "name": "data quality ml",
            "required_parameters": {
              "dataset_id": "dataset_14"
            }
          }
        ]
      }
    ]
  }

```

  ##### Ingestion Tasks 
```
{
    "integration_tasks": [
      {
        "task_id": "integration_task_1",
        "task_type": "ingestion",
        "source_dataset_id": "dataset_1",
        "target_dataset_id": "dataset_11",
        "ingestion_pattern": {
            "loader": "spark",
            "source_type": "local delim file", 
            "target_type": "spark table", 
            "load_type": "full", 
            "idempotent": true 
        } 
      },
      {
        "task_id": "integration_task_2",
        "task_type": "ingestion",
        "source_dataset_id": "dataset_2",
        "target_dataset_id": "dataset_12",
        "ingestion_pattern": {
            "loader": "spark",
            "source_type": "local delim file", 
            "target_type": "spark table", 
            "load_type": "incremental", 
            "idempotent": true 
        } 
      },
      {
        "task_id": "integration_task_3",
        "task_type": "ingestion",
        "source_dataset_id": "dataset_3",
        "target_dataset_id": "dataset_13",
        "ingestion_pattern": {
            "loader": "spark",
            "source_type": "local delim file", 
            "target_type": "spark table", 
            "load_type": "incremental", 
            "idempotent": true 
        } 
      },
      {
        "task_id": "integration_task_11",
        "task_type": "distribution",
        "source_dataset_id": "dataset_4",
        "target_dataset_id": "dataset_14",
        "distribution_pattern": {
            "extracter": "spark",
            "source_type": "spark sql file", 
            "target_type": "local delim file" 
        } 
      }      
    ]
  }
      
```

### Sample Output 

  ##### Schema 
```
root
 |-- account_id: string (nullable = true)
 |-- asset_id: string (nullable = true)
 |-- asset_value: string (nullable = true)
 |-- effective_date: string (nullable = true)

```

  ##### Sample Data 
```
+----------+--------+-----------+--------------+
|account_id|asset_id|asset_value|effective_date|
+----------+--------+-----------+--------------+
|      ACC1|       1|     -35000|    2024-12-26|
|      ACC1|       2|     -15000|    2024-12-26|
+----------+--------+-----------+--------------+
only showing top 2 rows

```

  ##### Validation 
```
Load is successful. Source Record Count = 9, Target Record Count = 9

```
