# df-data-ingestion

This application runs a data ingestion workflow to ingest a delimited file into a spark table. Application can be invoked using CLI or REST API end points. This allows the app to be integrated into a larger data ingestion / distribution framework.

### Install

- **Install via setuptools**:
  ```sh
    python setup.py install
  ```

### Usage Examples

- **Run a ingestion workflow via CLI**:
  ```sh
    ingest_app run-ingestion-workflow --ingestion_workflow_id "2" --env "dev"
  ```

- **Run a ingestion workflow via API**:
  ##### Start the API server
  ```sh
    python ingest_app/ingest_app_api.py
  ```
  ##### Invoke the API endpoint
  ```sh
    https://<host name with port number>/run-ingestion-workflow/{ingestion_workflow_id}
    https://<host name with port number>/run-ingestion-workflow/2
  ```
  ##### Invoke the API from Swagger Docs interface
  ```sh
    https://<host name with port number>/docs

    /run-ingestion-workflow/{dataset_id}
    /run-ingestion-workflow/2
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
These are metadata that would be captured via the DQ application UI and stored in a database.

  ##### datasets 
```

```

### Sample Output 

