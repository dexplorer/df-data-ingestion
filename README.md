# df-data-ingestion

This application runs a data ingestion workflow to ingest a delimited file into a spark table. 

It runs the configured pre-ingestion (data quality checks, data profiling) and post-ingestion (data reconciliation) tasks as part of the workflow.

Application can be invoked using CLI or REST API end points. This allows the app to be integrated into a larger data ingestion / distribution framework.

### Install

- **Install via Makefile and pip**:
  ```
    make install
  ```

### Usage Examples

- **Run a ingestion workflow via CLI**:
  ```sh
    ingest-app-cli run-ingestion-workflow --ingestion_workflow_id "2" --env "dev"
  ```

- **Run a ingestion workflow via CLI with cycle date override**:
  ```sh
    ingest-app-cli run-ingestion-workflow --ingestion_workflow_id "2" --env "dev" --cycle_date "2024-12-24"
  ```

- **Run a ingestion workflow via API**:
  ##### Start the API server
  ```sh
    ingest-app-api --env "dev"
  ```
  ##### Invoke the API endpoint
  ```sh
    https://<host name with port number>/run-ingestion-workflow/?ingestion_workflow_id=<value>
    https://<host name with port number>/run-ingestion-workflow/?ingestion_workflow_id=<value>&cycle_date=<value>

    /run-ingestion-workflow/?ingestion_workflow_id=3
    /run-ingestion-workflow/?ingestion_workflow_id=1&cycle_date=2024-12-26
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
        "dataset_id": "2",
        "catalog_ind": true,
        "file_delim": ",",
        "file_path": "APP_ROOT_DIR/data/acct_positions_yyyymmdd.csv",
        "schedule_id": "2",
        "dq_rule_ids": [], 
        "model_parameters": {
          "features": [
            {
              "column": "account_id",
              "variable_type": "category",
              "variable_sub_type": "nominal",
              "encoding": "frequency"
            },
            {
              "column": "asset_id",
              "variable_type": "category",
              "variable_sub_type": "nominal",
              "encoding": "one hot"
            },
            {
              "column": "asset_value",
              "variable_type": "numeric",
              "variable_sub_type": "float",
              "encoding": "numeric"
            }
          ],
          "hist_data_snapshots": [
            {
              "snapshot": "t-1d"
            },
            {
              "snapshot": "lme"
            }
          ],
          "sample_size": 10000
        }, 
        "recon_file_delim": "|", 
        "recon_file_path": "APP_ROOT_DIR/data/acct_positions_yyyymmdd.recon" 
      },
      {
        "dataset_id": "12",
        "catalog_ind": true,
        "schedule_id": null, 
        "dq_rule_ids": null, 
        "model_parameters": null, 
        "database_name": "dl_asset_mgmt", 
        "table_name": "tacct_pos", 
        "partition_keys": [
          "effective_date" 
        ]
      }
    ]
  }

```

  ##### Ingestion Workflows 
```
{
    "ingestion_workflows": [
      {
        "ingestion_workflow_id": "2",
        "ingestion_task_id": "2",
        "pre_ingestion_tasks": [
          {
            "name": "data quality",
            "required_parameters": {
              "dataset_id": "2"
            }
          },
          {
            "name": "data quality ml",
            "required_parameters": {
              "dataset_id": "2"
            }
          },
          {
            "name": "data profile",
            "required_parameters": {
              "dataset_id": "2"
            }
          }
        ],
        "post_ingestion_tasks": [
          {
            "name": "data recon",
            "required_parameters": {
              "dataset_id": "12"
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
    "ingestion_tasks": [
      {
        "ingestion_task_id": "1",
        "source_dataset_id": "1",
        "target_dataset_id": "11",
        "ingestion_pattern": {
            "loader": "spark",
            "source_type": "delimited file", 
            "target_type": "spark table", 
            "load_type": "full", 
            "idempotent": true 
        } 
      },
      {
        "ingestion_task_id": "2",
        "source_dataset_id": "2",
        "target_dataset_id": "12",
        "ingestion_pattern": {
            "loader": "spark",
            "source_type": "delimited file", 
            "target_type": "spark table", 
            "load_type": "incremental", 
            "idempotent": true 
        } 
      },
      {
        "ingestion_task_id": "3",
        "source_dataset_id": "3",
        "target_dataset_id": "13",
        "ingestion_pattern": {
            "loader": "spark",
            "source_type": "delimited file", 
            "target_type": "spark table", 
            "load_type": "incremental", 
            "idempotent": true 
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
