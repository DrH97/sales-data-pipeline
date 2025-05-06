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


## Reporting
- A file with sql scripts for the different reports is placed under the db directory
- Price data is not provided thus revenue isn't calculated, however, quantities are summarized and given prices data, it can easily be improved on


## Challenges and workarounds
1. Glue connection to RDS refused to connect and had to debug for quite a while. Continuing to review the issue
2. Because of that, the ETL does not load the data to RDS and instead populates the catalogue tables temporarily