# df-data-ingestion

This application runs a data ingestion workflow to ingest a delimited file into a spark table. 

It runs the configured pre-ingestion (data quality checks, data profiling) and post-ingestion (data reconciliation) tasks as part of the workflow.

Also, it runs the data lineage task to capture the relationships. This is not optional.

Application can be invoked using CLI or REST API end points. This allows the app to be integrated into a larger data ingestion / distribution framework.

### Install

- **Install via Makefile and pip**:
  ```
    make install
  ```

### Usage Examples

- **Run a ingestion workflow via CLI**:
  ```sh
    ingest-app-cli run-ingestion-workflow --ingestion_workflow_id "1" --env "dev"
    ingest-app-cli run-ingestion-workflow --ingestion_workflow_id "2" --env "dev"
    ingest-app-cli run-ingestion-workflow --ingestion_workflow_id "3" --env "dev"
  ```

- **Run a ingestion workflow via CLI with cycle date override**:
  ```sh
    ingest-app-cli run-ingestion-workflow --ingestion_workflow_id "1" --env "dev" --cycle_date "2024-12-24"
    ingest-app-cli run-ingestion-workflow --ingestion_workflow_id "2" --env "dev" --cycle_date "2024-12-24"
    ingest-app-cli run-ingestion-workflow --ingestion_workflow_id "3" --env "dev" --cycle_date "2024-12-24"
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

    /run-ingestion-workflow/?ingestion_workflow_id=1
    /run-ingestion-workflow/?ingestion_workflow_id=2
    /run-ingestion-workflow/?ingestion_workflow_id=3
    /run-ingestion-workflow/?ingestion_workflow_id=1&cycle_date=2024-12-26
    /run-ingestion-workflow/?ingestion_workflow_id=2&cycle_date=2024-12-26
    /run-ingestion-workflow/?ingestion_workflow_id=3&cycle_date=2024-12-26
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
      "dataset_id": "1",
      "dataset_type": "local delim file",
      "catalog_ind": true,
      "file_delim": ",",
      "file_path": "APP_DATA_IN_DIR/assets_yyyymmdd.csv",
      "schedule_id": "2",
      "recon_file_delim": "|",
      "recon_file_path": "APP_DATA_IN_DIR/assets_yyyymmdd.recon"
    },
    {
      "dataset_id": "2",
      "dataset_type": "local delim file",
      "catalog_ind": true,
      "file_delim": ",",
      "file_path": "APP_DATA_IN_DIR/acct_positions_yyyymmdd.csv",
      "schedule_id": "2",
      "recon_file_delim": "|",
      "recon_file_path": "APP_DATA_IN_DIR/acct_positions_yyyymmdd.recon"
    },
    {
      "dataset_id": "3",
      "dataset_type": "local delim file",
      "catalog_ind": true,
      "file_delim": ",",
      "file_path": "APP_DATA_IN_DIR/customers_yyyymmdd.csv",
      "schedule_id": "2",
      "recon_file_delim": "|",
      "recon_file_path": "APP_DATA_IN_DIR/customers_yyyymmdd.recon"
    },
    {
      "dataset_id": "11",
      "dataset_type": "spark table",
      "catalog_ind": true,
      "schedule_id": "2",
      "database_name": "dl_asset_mgmt",
      "table_name": "tasset",
      "partition_keys": [],
      "recon_file_delim": "|",
      "recon_file_path": "APP_DATA_IN_DIR/assets_yyyymmdd.recon"
    },
    {
      "dataset_id": "12",
      "dataset_type": "spark table",
      "catalog_ind": true,
      "schedule_id": "2",
      "database_name": "dl_asset_mgmt",
      "table_name": "tacct_pos",
      "partition_keys": [
        "effective_date"
      ],
      "recon_file_delim": "|",
      "recon_file_path": "APP_DATA_IN_DIR/acct_positions_yyyymmdd.recon"
    }
  ]
}

```

  ##### Ingestion Workflows 
```
{
    "workflows": [
      {
        "workflow_id": "1",
        "workflow_type": "ingestion", 
        "ingestion_task_id": "1",
        "pre_tasks": [
          {
            "name": "data quality",
            "required_parameters": {
              "dataset_id": "1"
            }
          },
          {
            "name": "data profile",
            "required_parameters": {
              "dataset_id": "1"
            }
          }
        ],
        "post_tasks": [
        ]
      },
      {
        "workflow_id": "2",
        "workflow_type": "ingestion", 
        "ingestion_task_id": "2",
        "pre_tasks": [
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
        "post_tasks": [
          {
            "name": "data reconciliation",
            "required_parameters": {
              "dataset_id": "12"
            }
          }
        ]
      },
      {
        "workflow_id": "3",
        "workflow_type": "ingestion", 
        "ingestion_task_id": "3",
        "pre_tasks": [
          {
            "name": "data quality",
            "required_parameters": {
              "dataset_id": "3"
            }
          },
          {
            "name": "data profile",
            "required_parameters": {
              "dataset_id": "3"
            }
          }
        ],
        "post_tasks": [
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
        "task_id": "1",
        "task_type": "ingestion",
        "source_dataset_id": "1",
        "target_dataset_id": "11",
        "ingestion_pattern": {
            "loader": "spark",
            "source_type": "local delim file", 
            "target_type": "spark table", 
            "load_type": "full", 
            "idempotent": true 
        } 
      },
      {
        "task_id": "2",
        "task_type": "ingestion",
        "source_dataset_id": "2",
        "target_dataset_id": "12",
        "ingestion_pattern": {
            "loader": "spark",
            "source_type": "local delim file", 
            "target_type": "spark table", 
            "load_type": "incremental", 
            "idempotent": true 
        } 
      },
      {
        "task_id": "3",
        "task_type": "ingestion",
        "source_dataset_id": "3",
        "target_dataset_id": "13",
        "ingestion_pattern": {
            "loader": "spark",
            "source_type": "local delim file", 
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
