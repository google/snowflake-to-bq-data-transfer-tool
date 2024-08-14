# Snowflake To BigQuery Data Transfer User Guide [SFBQDT]

* For SFBQDT detailed documentation, please refer to [docs/DOCUMENTATION.md](docs/DOCUMENTATION.md)
* For SFBQDT supported APIs details, please refer to [docs/REST_API.md](docs/REST_API.md)
* For SFBQDT supported/non-supported datatype, please refer to [docs/DATATYPES.md](docs/DATATYPES.md)


This tool helps in transferring(migrating) the Snowflake data(Schema,Table) to BigQuery. \
It can help in automating the historical load migration.

## Prerequisites:
* [gcloud CLI](https://cloud.google.com/sdk/gcloud)
* Maven
* Java 11
* GCP project
* Snowflake project
* Snowflake Object/Resources
* Appropriate access to Snowflake account
* Clone the repository

# Steps

## Snowflake Object Creation

The tool utilizes several Snowflake entities, such as databases, tables, roles, users, and warehouses. Databases, tables, and schemas should \
ideally be pre-existing, but users have the flexibility to establish distinct roles, users, and warehouses specifically for migration purposes. \
This helps ensure segregation from other entities in the environment. The code includes a straightforward script that can execute these tasks. \
This script is provided as a reference for generating necessary users, roles, and other entities. Prior to any action, it's recommended to seek \
guidance from your enterprise's security and administrative teams, as the script is intended for reference purposes only.

**[Snowflake Object Creation Script](docs/snowflake-object-creation.txt)**

## Setting up authentication
Below steps can be followed for setting up authentication & authorization:
* Oauth Token Generation
```
CREATE or REPLACE SECURITY INTEGRATION OAUTH_FOR_REST_API
  TYPE = OAUTH
  ENABLED = TRUE
  OAUTH_CLIENT = CUSTOM
  OAUTH_CLIENT_TYPE = <Appropriate Value>
  OAUTH_REDIRECT_URI = 'http://locahost:8080'
  OAUTH_ISSUE_REFRESH_TOKENS = TRUE
  OAUTH_REFRESH_TOKEN_VALIDITY = 86400
  OAUTH_ALLOW_NON_TLS_REDIRECT_URI=true; --(This line is not needed if redirect URL is https)
```
* Fetch the clientId and secret from Snowflake
``` 
select SYSTEM$SHOW_OAUTH_CLIENT_SECRETS( 'OAUTH_FOR_REST_API' )
```
<img src="images/img.png" alt="Alt Text" height="100" width="800">

* Generate the one time access token using the above clientId, secret, snowflake URL( ```https://<account>.snowflakecomputing.com/api/v2/statements/```) \
and postman. Below query can be used to find the values of Auth URL, Access Token URL and other details.
```
 DESC SECURITY INTEGRATION OAUTH_FOR_REST_API;
```
OAUTH_AUTHORIZATION_ENDPOINT=Auth URL and OAUTH_TOKEN_ENDPOINT= Access Token URL of postman.

<img src="images/access-token.png" alt="Alt Text" height="400" width="750">

* Copy the refresh token from the generated token after authenticating using the Snowflake credentials.

<img src="images/refresh-token.png" alt="Alt Text" height="300" width="600">

* This refresh token will be encrypted in the tool, tool will keep on refreshing the token  and get new access token until the max duration \ 
  which is set in security integration against key OAUTH_REFRESH_TOKEN_VALIDITY expires.

# Quick Start JAR
For a deployment tailored to specific requirements, building the JAR file from source code using custom properties is advised.
For a quick trial or exploration, running the provided JAR file is also possible.

To run the tool use the [cloud shell](https://cloud.google.com/shell/docs/launching-cloud-shell#launch_from_the) terminal. It has all the
pre-requisites.

### Download repo and prebuilt jar.
```
# in cloud shell terminal
gcloud auth application-default login
wget https://github.com/GoogleCloudPlatform

# in cloud shell terminal
wget https://github.com/google/snowflake-to-bq-data-transfer-tool/releases/download/v.1.0.0/snowflake-to-bq-data-transfer.jar
```

Run tool for simple inline query
```
# in cloud shell terminal
java -jar snowflake-to-bq-data-transfer-1.0.0.jar --spring.datasource.h2.url=./app_data \
--snowflake.account.url={SNOWFLAKE_URL} --gcs.storage.integration=MIGRATION_INTEGRATION --service.account.file.path={Path of service account}"
```
* **service.account.file.path:** This property can be skipped if running using user account.

## Custom Jar Build
* Update the [application.properties](src/main/resources/application.properties) file based on your Snowflake instance and need. The properties file contains \
  descriptions for each property. 
* Modify the files' [snowflake_request_body.json](src/main/resources/snowflake_request_body.json) and [snowflake_table_query_mapping.json](src/main/resources/snowflake_table_query_mapping.json) based
  on the needs else skip it.
* Build the code from src and pom.xml directory level
```
mvn clean package 
```

## Launch The Custom Jar
* Once built, the tool can be launched with the command  ```java -jar {path to the jar}```. The executable JAR, found in the target folder after \
  the Build tool steps, runs on any JRE 11+ environment.
* The tool supports dynamic property configuration at startup, eliminating the need for repetitive JAR file rebuilds.
* Example
```
java -jar {PATH OF JAR } --snowflake.table.query.mapping.path=/Users/tests/snowflake_table_query_mapping.json \
--snowflake.request.body.json.path=/Users/tests/snowflake_request_body.json --snowflake.account.url=https://<account-name>.us-east-1.snowflakecomputing.com \
--spring.datasource.h2.url=/Users/tests/db ....
```
* Upon startup, the tool is ready to process REST calls. Users can leverage tools like Postman or curl commands to interact with the exposed REST API endpoints.

## APIs
SFBQDT offers various APIs designed for data transfer purposes. To explore these APIs in depth, please refer to the provided documentation [docs/REST_API.md](docs/REST_API.md)

## State Management

The tool maintains execution state, enabling it to restart from the last successful point should any failures occur. It utilizes an embedded [H2 Database](https://www.h2database.com/html/main.html), \
storing data in a file at the location defined within the 'application.properties' file.

### Connect to H2 Database
* User can connect to H2 Database using the link ```http://localhost:8080/h2-console/login.do``` after starting the tool on same machine.
* User can key in the required credential which is set in application.properties. Default username and password is **SF_EMBEDDED/SF_EMBEDDED**

  <img src="images/h2-login-page.png" alt="Alt Text" height="300" width="400">

* All data is stored within the table named **APPLICATION_DATA**. Below is the schema for the example table:

| COLUMN_NAME                       | DATA_TYPE         | CHARACTER_MAXIMUM_LENGTH | IS_NULLABLE |
|-----------------------------------|-------------------|--------------------------|-------------|
| ID                                | BIGINT            |                          | NO          |
| BQ_LOAD_FORMAT                    | CHARACTER VARYING | 255                      | YES         |
| CREATED_TIME                      | CHARACTER VARYING | 255                      | YES         |
| GCS_BUCKET_FOR_DDLS               | CHARACTER VARYING | 255                      | YES         |
| GCS_BUCKET_FOR_TRANSLATION        | CHARACTER VARYING | 255                      | YES         |
| IS_TABLE_CREATED                  | BOOLEAN           |                          | YES         |
| IS_DATA_LOADED_IN_BQ              | BOOLEAN           |                          | YES         |
| IS_DATA_UNLOADED_FROM_SNOWFLAKE   | BOOLEAN           |                          | YES         |
| IS_ROW_PROCESSING_DONE            | BOOLEAN           |                          | YES         |
| IS_SCHEMA                         | BOOLEAN           |                          | YES         |
| IS_SOURCE_DDL_COPIED              | BOOLEAN           |                          | YES         |
| IS_TRANSLATED_DDL_COPIED          | BOOLEAN           |                          | YES         |
| LAST_UPDATED_TIME                 | CHARACTER VARYING | 255                      | YES         |
| LOCATION                          | CHARACTER VARYING | 255                      | YES         |
| REQUEST_LOG_ID                    | CHARACTER VARYING | 255                      | YES         |
| SNOWFLAKE_FILE_FORMAT             | CHARACTER VARYING | 255                      | YES         |
| SNOWFLAKE_STAGE_LOCATION          | CHARACTER VARYING | 255                      | YES         |
| SNOWFLAKE_STATEMENT_HANDLE        | CHARACTER VARYING | 255                      | YES         |
| SOURCE_DATABASE_NAME              | CHARACTER VARYING | 255                      | YES         |
| SOURCE_SCHEMA_NAME                | CHARACTER VARYING | 255                      | YES         |
| SOURCE_TABLE_NAME                 | CHARACTER VARYING | 255                      | YES         |
| TARGET_DATABASE_NAME              | CHARACTER VARYING | 255                      | YES         |
| TARGET_SCHEMA_NAME                | CHARACTER VARYING | 255                      | YES         |
| TARGET_TABLE_NAME                 | CHARACTER VARYING | 255                      | YES         |
| TRANSLATED_DDL_GCS_PATH           | CHARACTER VARYING | 255                      | YES         |
| WAREHOUSE                         | CHARACTER VARYING | 255                      | YES         |
| WORKFLOW_NAME                     | CHARACTER VARYING | 255                      | YES         |

# Disclaimer
This is not an officially supported Google product.