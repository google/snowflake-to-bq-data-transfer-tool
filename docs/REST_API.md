# Snowflake To BigQuery Data Transfer Supported APIs Guide [STBQDT]

* For STBQDT introduction, please refer to [/README.md](../README.md)
* For STBQDT detailed documentation, please refer to [/DOCUMENTATION.md](DOCUMENTATION.md)
* For STBQDT supported/non-supported datatype, please refer to [/DATATYPES.md](DATATYPES.md)

# REST APIs


## Setting Access Token
* Refresh Token generated from the step [Setting up authentication](../README.md#setting-up-authentication) present in file(**docs/README.md**) should be set in the connector, \
  connector will automatically keep on refreshing it until the max expiry of refresh token. Connector keeps the value in encrypted format in a map.
* Request URL and body
```
Request Type: POST
URL: http://localhost:8080/connector/save-oauth-values
BODY: 
        { "clientId":"79dAYOORu",
        "clientSecret":"MJONWzHYmtkrqdI",
        "refreshToken":"ver:2-hint:444605079557-did:"
        }
```
* **CURL command**
```
curl --location 'http://localhost:8080/connector/save-oauth-values' \
--header 'Content-Type: application/json' \
--data '{"clientId":"79dAYOORu",
"clientSecret":"MJONWzHYmtkrqdI",
"refreshToken":"ver:2-hint:444605079557-did:"
}'
```
* Change the local host to the appropriate ip_address if connector is not started in same machine where the request are being executed.
* Above method will set the token in runtime, and it can be updated anytime when application is running. Initially generated access token \
  remains active for 10 min so make sure to execute this steps with in 10 min.
* Token value will be encrypted by application before use and will not be printed in the connector logs.

## Migrate Data
* Data migration can be started using another rest API.
* Before executing this API we need to have below resources created.
* GCS buckets for storing ddls, translated ddls and snowflake tables. We can use one bucket for all the resources if required, Connector \
  will create "**snowflake-ddls**","**translated-snowflake-ddls**","**backup-ddls**" with in the bucket to store **ddls**, **translated ddls**\
  and **backup of old ddls** respectively.
* BigQuery dataset. Table can be created beforehand or let it be created by connector.
* Snowflake STORAGE INTEGRATION and access to use it by specific user(which will be used for OAuth token) for creating stage at runtime. \
  Command is given in the **[Script file](docs/snowflake-object-creation.txt)**
* **Request URL and Body**
```
Request Type: POST
URL: http://localhost:8080/connector/migrate-data
BODY: 
         {
            "sourceDatabaseName": "TEST_DATABASE",
            "sourceSchemaName": "public",
            "sourceTableName": "orders",
            "targetDatabaseName": "terragrunt-test2",
            "targetSchemaName": "test_dataset",
            "schema": false,
            "bqTableExists": true,
            "gcsBucketForDDLs": "snowflake-to-gcs-migration",
            "gcsBucketForTranslation": "snowflake-to-gcs-migration",
            "location": "us",
            "snowflakeStageLocation":        "snowflake-to-gcs-migration/data-unload",
            "snowflakeFileFormatValue":"SF_GCS_PARQUET_FORMAT1",
            "bqLoadFileFormat":"PARQUET"
         }



Note: Set schema:true, to migrate all the tables present in the schema.
```
* **Definition of body parameters**
```
sourceDatabaseName: Database name in Snowflake to be migrated to BigQuery.
sourceSchemaName: Schema name in a database to be migrated.
sourceTableName: Table names to be migrated, if full schema is not getting migrated, user can give comma separated value to migrate more than 1 table.
targetDatabaseName: BigQuery does not have database concept, this is basically the project name in GCP.
targetSchemaName: Dataset name in BigQuery.
schema: This parameter will define if we are migrating the full schema or not. True means all the tables in the schema will be migrated without 
        even providing the table names using parameter "sourceTableName". It gets preference over "sourceTableName", if this is true then table name will be ignored.
bqTableExists: If true, connector will assume that table is already created beforehand, if false connector will create the table before loading.
gcsBucketForDDLs: This GCS bucket will hold the DDLS extracted from Snowflake for the table name.
gcsBucketForTranslation: This GCS bucket will hold the translated DDLs which will be used to create table in BigQuery.
location: location to be used for running translation job and bigquery load
snowflakeStageLocation: This is basically the storage location, its gets used in creating stage. Different stage prepend this value to other dynamic values.
                        All the data files from Snowflake will go inside this folder location.
snowflakeFileFormatValue: The format in which data from Snowflake table will be unloaded to GCS, it should be created beforehand in Snowflake
bqLoadFileFormat: Load format user by bq load job, it should be compatable with snowflakeFileFormatValue.

```
* **CURL command**
```
curl --location 'http://localhost:8080/connector/migrate-data' \
--header 'Content-Type: application/json' \
--data '{
	"sourceDatabaseName": "TEST_DATABASE",
	"sourceSchemaName": "public",
	"sourceTableName": "orders",
	"targetDatabaseName": "terragrunt-test2",
	"targetSchemaName": "test_dataset",
	"schema": false,
	"bqTableExists": true,
	"gcsBucketForDDLs": "snowflake-to-gcs-migration",
	"gcsBucketForTranslation": "snowflake-to-gcs-migration",
	"location": "us",
	"snowflakeStageLocation": "snowflake-to-gcs-migration/data-unload",
    "snowflakeFileFormatValue":"SF_GCS_CSV_FORMAT1",
    "bqLoadFileFormat":"CSV"
}'

```
* This rest API when executed will perform below operations
    * Extract the ddls for the table name from Snowflake.
    * Translate the ddls using translation service to use in BigQuery.
    * export the data from Snowflake to GCS bucket.
    * Create the table in BigQuery if not exists and load the GCS data to BigQuery.
    * Execute to export from Snowflake and load in BigQuery for each table in parallel.
    * Maintain the state of each operation in embedded database to provide restartability in case of failure.

## Extract and Translate DDL
* **Request URL and Body**
```
Request Type: POST
URL: http://localhost:8080/connector/extract-ddl
BODY: 
         {
          "sourceDatabaseName": "TEST_DATABASE",
          "sourceSchemaName": "public",
          "sourceTableName": "DATES_VALUE",
          "targetDatabaseName": "terragrunt-test2",
          "targetSchemaName": "test_dataset",
          "schema": false,
          "gcsBucketForDDLs": "snowflake-to-gcs-migration",
          "gcsBucketForTranslation": "snowflake-to-gcs-migration",
          "location": "us"
          }
```
* **Definition of body parameters**
```
sourceDatabaseName: Database name in Snowflake to be migrated to BigQuery.
sourceSchemaName: Schema name in a database to be migrated.
sourceTableName: Table names to be migrated, if full schema is not getting migrated, user can give comma separated value to migrate more than 1 table.
targetDatabaseName: BigQuery does not have database concept, this is basically the project name in GCP.
targetSchemaName: Dataset name in BigQuery.
schema: This parameter will define if we are migrating the full schema or not. True means all the tables in the schema will be migrated without 
        even providing the table names using parameter "sourceTableName". It gets preference over "sourceTableName", if this is true then table name will be ignored.
gcsBucketForDDLs: This GCS bucket will hold the DDLS extracted from Snowflake for the table name.
gcsBucketForTranslation: This GCS bucket will hold the translated DDLs which will be used to create table in BigQuery.
location: location to be used for running translation job and bigquery load
```
* **CURL command**
```
curl --location --request POST 'http://localhost:8080/connector/extract-ddl' \
--header 'Authorization: Bearer ver:1-hint:29137637956804618-ETMsDgAAAYsBnhlIABR' \
--header 'Content-Type: application/json' \
--data-raw '{
	"sourceDatabaseName": "TEST_DATABASE",
	"sourceSchemaName": "public",
	"sourceTableName": "DATES_VALUE",
	"targetDatabaseName": "terragrunt-test2",
	"targetSchemaName": "test_dataset",
	"gcsBucketForDDLs": "snowflake-to-gcs-migration",
    "schema": false,
	"gcsBucketForTranslation": "snowflake-to-gcs-migration",
	"location": "us"
}
'

```
* This rest API when executed will perform below operations
    * Extract the ddls for the table name from Snowflake.
    * Translate the ddls using translation service to use in BigQuery.
    * Write the result to GCS bucket.
    * It can be valuable if the user wishes to independently create a table after verifying the translation.

## Snowflake Table Export to GCS
* **Request URL and Body**
```
Request Type: POST
URL: http://localhost:8080/connector/snowflake-unload-to-gcs
BODY: 
         {
          "tableNames":["orders"],
          "snowflakeStageLocation": "snowflake-to-gcs-migration/data-unload",
          "snowflakeStageLocation": "SF_GCS_CSV_FORMAT1"
          }

```
* **Definition of body parameters**
```
tableNames: Name of the table which needs to be exported.
snowflakeStageLocation: This is basically the storage location, its gets used in creating stage. Different stage prepend this value to other dynamic values.
                        All the data files from Snowflake will go inside this folder location.
snowflakeFileFormatValue: The format in which data from Snowflake table will be unloaded to GCS, it should be created beforehand in Snowflake.
```
* **CURL command**
```
curl --location --request POST 'http://localhost:8080/connector/snowflake-unload-to-gcs' \
--header 'Authorization: Bearer ver:1-hint:29137637956804618-ETMsDgAAAYsB' \
--header 'Content-Type: application/json' \
--data-raw '{
"tableNames":["orders","teststs"],
"snowflakeStageLocation": "snowflake-to-gcs-migration/data-unload",
"snowflakeFileFormatValue": "SF_GCS_PARQUET_FORMAT1"
}
'

```
* This REST API when executed will perform exporting of table data from Snowflake to GCS.

## Process Failed Requests
* **Request URL and Body**
```
Request Type: GET
URL: http://localhost:8080/connector/process-failed-request

```
* **CURL command**
```
curl --location --request GET 'http://localhost:8080/connector/process-failed-request' \
--header 'Authorization: Bearer ver:1-hint:29137637956804618-ETMsDgAAAYsBnhlIABRBRVMv'
'

```
* When this REST API is invoked, it will reprocess requests that previously failed during the 'migrate-data' API execution, potentially due to errors.\
  The data associated with these requests is stored in the H2 Database. Essentially, this API reprocesses requests for which the Snowflake Export \
  operation was successfully completed. Since the Snowflake Export operation is resource-intensive and costly, it should not be redundantly executed.\
  Conversely, other steps such as extracting DDL and translating DDL are lightweight and can be retried in case of failure. If the Snowflake operation \
  has not yet completed, this API will disregard those pending requests.


* Connector contains a JSON [snowflake_table_query_mapping.json](src/main/resources/snowflake_table_query_mapping.json) which maintains a mapping of table and query.\
  If given query will be used for exporting the data from Snowflakes for the given table. Without this entry Connector exports \
  the data based on full table schema. This will be helpful when user does not want to export all the columns or apply some transformation for any column,\
  like casting timestamp to string, or excluding PII/PHI column etc.

# Disclaimer
This is not an officially supported Google product.