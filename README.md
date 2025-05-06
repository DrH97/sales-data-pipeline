# Sales Data Pipeline

A simple ingestion pipeline from S3 to RDS

## AWS Set Up

### S3
- Create an S3 bucket and give it a unique name ```bucket_name```
- Create a folder structure as follows:
```
└── bucket_name
    ├── athena
    ├── catalogue
    ├── raw_data
    │   └──  sales_data.csv 
    ├── scripts  
    └── temporary
```

- Upload the csv in the data directory to the s3 'raw_data' folder
- Upload the scripts directory files to the s3 'scripts' folder

### IAM Role
- Create an IAM role to be used for the pipeline as ```role_name```
- Attach the following policies from IAM 
    ```
    AmazonS3FullAccess
    AWSGlueServiceRole
    AmazonRDSFullAccess
  ```
- Alternatively attach the ```AdministratorAccess``` to have full access (not recommended)

### RDS
- Create an RDS server instance and select postgres as the engine.
- Choose the configurations needed or work with the defaults
- Configure the database using the schema defined in db/schema.sql

### Glue
- Create a glue service and create a new ETL using the configs defined in Sales_Data_Pipeline_ETL
- Create a catalogue database to be used to reference the rds database
- Add a connection to the RDS database
- Add a crawler to use the connection and populate the catalogue database tables

## ETL Flow
The ETL flows as follows:
1. Load data from S3 bucket
2. Update column names: use standard snake case
3. Remove null rows
4. Using existing table records and raw data, get unique records and update tables e.g. apply a left anti-join to (warehouses from raw data, warehouses in database), then insert new records
5. Create normalised sales data by joining records of tables and raw data
6. Insert sales data (Pending: whether to add a unique check for records before insert)


## Reporting
- A file with SQL scripts for the different reports is placed under the db directory
- Price data is not provided, thus revenue isn't calculated, however, quantities are summarized, and given prices data, it can easily be improved on


## Challenges and workarounds
1. ~~Glue connection to RDS refused to connect and had to debug for quite a while. Continuing to review the issue~~ **Resolved** (security groups config)
2. ~~Because of that, the ETL does not load the data to RDS and instead populates the catalogue tables temporarily~~ **Resolved**
3. With an empty DB, the ETL needs to be executed twice to first populate the dependency tables before sales data can be populated in a normalised format. **Proposed solution:** edit the script to have the sales data insertion to come after all other data is populated
4. Review usage of other fields as the primary key instead of autogen id since they are utilised in multiple joins