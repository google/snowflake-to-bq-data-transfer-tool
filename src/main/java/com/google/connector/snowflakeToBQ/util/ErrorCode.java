/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.connector.snowflakeToBQ.util;

/** ENUM class to define all the error code use in the application */
public enum ErrorCode {
  SNOWFLAKE_CONFIG_LOADER(1000, "Error: Loading Snowflake config data file"),
  SNOWFLAKE_UNLOAD_DATA_ERROR(1001, "Error: Unloading data from Snowflake"),
  TOKEN_REFRESH_ERROR(
      1002,
      "Error: Unable to refresh token based on the received paramaters(client_id,"
          + " clientSecret,TokenUrl etc)"),
  TABLE_CREATION_ERROR(1003, "Error: Creating table in BigQuery"),
  BQ_QUERY_JOB_EXECUTION_ERROR(1004, "Error: During the BigQuery Job execution"),
  ROW_UNIQUENESS_VOILATION_ERROR(
      1005, "Error: Inserting the row in table as datatabse , schema and tablename is same"),
  SNOWFLAKE_REST_API_POLL_ERROR(
      1006,
      "Error: During polling the initiated rest API call.It could be from application, due to"
          + " exceeded max attempt with in given duration or from Snowflake"),
  TABLE_ALREADY_EXISTS(
      1007, "Error: table already existing, hence application will not create the same table"),

  TABLE_NAME_NOT_PRESENT_IN_REQUEST(
      1008, "Error: Table name not present in the request to get ddl, please provide tablename"),
  SNOWFLAKE_RESPONSE_PARSING_ERROR(
      1009, "Error: Failed to parse the received JSON response from snowflake rest API execution"),
  MIGRATION_WORKFLOW_EXECUTION_ERROR(1010, "Error: Migration Workflow execution error"),
  JDBC_EXECUTION_EXCEPTION(1011, "Error: Executing the query in Snowflake using JDBC connection"),
  ENCRYPTION_ERROR(1012, "Error: Encrypting the values"),
  DECRYPTION_ERROR(1013, "Error: Decrypting the values"),
  TABLE_NOT_EXISTS(1014, "Error: table does not exists"),
  DDL_EXTRACTION_EXCEPTION(1015, "Error: Extracting DDL"),
  SNOWFLAKE_REST_API_EXECUTION_ERROR(1016, "Error: Snowflake rest API execution");

  private final int errorCode;
  private final String message;

  ErrorCode(int code, String message) {
    this.errorCode = code;
    this.message = message;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getMessage() {
    return message;
  }
}
