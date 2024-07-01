# Snowflake To BigQuery Data Transfer User Guide [STBQDT]

* For STBQDT detailed documentation, please refer to [docs/DOCUMENTATION.md](docs/DOCUMENTATION.md)
* For STBQDT supported APIs details, please refer to [docs/REST_API.md](docs/REST_API.md)
* For STBQDT supported/non-supported datatype, please refer to [docs/DATATYPES.md](docs/DATATYPES.md)


This tool helps in transferring(migrating) the Snowflake data(Schema,Table) to BigQuery. \
It can help in automating the historical load migration.

# Quick Start

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

**[Script file](docs/snowflake-object-creation.txt)**

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

## Build the tool
* Update the [application.properties](src/main/resources/application.properties) file based on your Snowflake instance and need. The properties file contains \
  descriptions for each property. 
* Modify the files' [snowflake_request_body.json](src/main/resources/snowflake_request_body.json) and [snowflake_table_query_mapping.json](src/main/resources/snowflake_table_query_mapping.json) based
  on the needs and the environment values.
* Build the code from src and pom.xml directory level
```
mvn clean package 
```

## Start the tool
* After building the tool, it can be started using the command ```java -jar {path to the jar}```. After following the step \
  mentioned in [Build tool](#build-tool) step, the tool jar will be generated in the target folder. This jar is \
  executable across any JRE 11-compatible environments.
* Tool allows you to provide property values during application startup, eliminating the need to repeatedly build the JAR file.
* Example
```
java -jar {PATH OF JAR } --snowflake.table.query.mapping.path=/Users/tests/snowflake_table_query_mapping.json \
--snowflake.request.body.json.path=/Users/tests/snowflake_request_body.json --snowflake.account.url=https://<account-name>.us-east-1.snowflakecomputing.com \
--jdbc.url=jdbc:snowflake://https:/<account-name>.us-east-1.snowflakecomputing.com --spring.datasource.h2.url=jdbc:h2:file:/Users/tests/db
```
* Once the tool is started, it can accept rest calls. User can use postman or curl command to execute the rest API calls.

## APIs
STBQDT provides different APIs for data transfer,for detailed information please refer to [docs/REST_API.md](docs/REST_API.md)


## State Management

The tool manages the execution state, allowing it to resume from the last successful step in case of failure. It employs the\
[H2 Database](https://www.h2database.com/html/main.html) in embedded mode, storing data in a file mode at the location specified in the application.properties file.

### Connect to H2 Database
* User can connect to H2 Database using the link ```http://localhost:8080/h2-console/login.do``` after starting the tool on same machine.
* User can key in the required credential which is set in application.properties.

  <img src="images/h2-login-page.png" alt="Alt Text" height="300" width="400">

* All data is stored within the table named **APPLICATION_DATA**.

  <img src="images/h2-table.png" alt="Alt Text" height="700" width="400">

  <img src="images/h2-table-data.png" alt="Alt Text" height="300" width="2000">

# Disclaimer
This is not an officially supported Google product.