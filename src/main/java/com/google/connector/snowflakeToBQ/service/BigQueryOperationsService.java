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

package com.google.connector.snowflakeToBQ.service;

import static com.google.connector.snowflakeToBQ.util.ErrorCode.*;

import com.google.cloud.bigquery.*;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.BigQueryDetailsDataDTO;
import com.google.connector.snowflakeToBQ.service.Instancecreator.BigQueryInstanceCreator;
import java.util.UUID;

import com.google.connector.snowflakeToBQ.service.bigqueryjoboptions.LoadJobFactory;
import com.google.connector.snowflakeToBQ.service.bigqueryjoboptions.LoadOption;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Class to provide methods which help in performing bigquery related operations like create table,
 * load job etc.
 */
@Service
public class BigQueryOperationsService {
  private static final Logger log = LoggerFactory.getLogger(BigQueryOperationsService.class);
  final BigQueryInstanceCreator bigQueryInstanceCreator;
  final LoadJobFactory loadJobFactory;

  public BigQueryOperationsService(
      BigQueryInstanceCreator bigQueryInstanceCreator, LoadJobFactory loadJobFactory) {
    this.bigQueryInstanceCreator = bigQueryInstanceCreator;
    this.loadJobFactory = loadJobFactory;
  }

  /**
   * Method to perform the load job in BigQuery.
   *
   * @param bigqueryDetailsDto required parameter for executing the load job.
   * @return @boolean status
   */
  public boolean loadBigQueryJob(BigQueryDetailsDataDTO bigqueryDetailsDto) {
    // Create the Translation Service client
    boolean returnValue = false;
    TableId tableId =
        TableId.of(
            bigqueryDetailsDto.getProjectId(),
            bigqueryDetailsDto.getDatasetId(),
            bigqueryDetailsDto.getTableName());

    // Validating if the table for which load job is to perform exists or not
    if (!isTableExists(bigqueryDetailsDto)) {
      log.error(
          "Error Message:{},Error Code:{}, table name:{}",
          TABLE_NOT_EXISTS.getMessage(),
          TABLE_NOT_EXISTS.getErrorCode(),
          bigqueryDetailsDto.getTableName());
      throw new SnowflakeConnectorException(
          TABLE_NOT_EXISTS.getMessage(), TABLE_NOT_EXISTS.getErrorCode());
    }
    // fetching the schema of the table
    Schema tableSchema =
        bigQueryInstanceCreator.getBigQueryClient().getTable(tableId).getDefinition().getSchema();

    String sourceURI =
        String.format(
            "gs://%s/%s/*",
            bigqueryDetailsDto.getSnowflakeDataUnloadGCSPath(), bigqueryDetailsDto.getTableName());

    // Getting the appropriate loadjobconfiguration object based on the csv format received in the
    // request.
    // It could be CSV, Parquet etc.
    LoadJobConfiguration loadConfig =
        loadJobFactory
            .createService(
                LoadOption.valueOf(bigqueryDetailsDto.getBqLoadFileFormat()), tableSchema)
            .createLoadJob(tableId, sourceURI);

    JobId jobId =
        JobId.newBuilder()
            .setJob("Snowflake_" + UUID.randomUUID())
            .setLocation(
                StringUtils.isBlank(bigqueryDetailsDto.getLocation())
                    ? "us"
                    : bigqueryDetailsDto.getLocation())
            .build();

    try {
      Job loadJob =
          bigQueryInstanceCreator
              .getBigQueryClient()
              .create(JobInfo.newBuilder(loadConfig).setJobId(jobId).build());
      // Waiting for job to finish, no options has been give so it will wait max 12 hour with
      // unlimited retry attempts, we can add the max timeout and initial delay later based on the
      // real world use case
      loadJob = loadJob.waitFor();

      if (loadJob == null) {
        log.error(
            "Error executing BigQuery load job for JobId::{} and application's row:{}, return load job object is null",
            jobId.getJob(),
            bigqueryDetailsDto.getUniqueIdentifier());
      } else if (loadJob.getStatus().getError() == null) {
        returnValue = true;
        log.info("Data loaded successfully.");
      } else {
        log.error("Error executing BigQuery load job: {}", loadJob.getStatus().getError());
      }
    } catch (Exception e) {
      log.error(
          "Error executing BigQuery load job for JobId::{} and application's row:{}, Error Message:{}\nStack Trace:",
          jobId.getJob(),
          bigqueryDetailsDto.getUniqueIdentifier(),
          e.getMessage(),
          e);
      throw new SnowflakeConnectorException(
          BQ_QUERY_JOB_EXECUTION_ERROR.getMessage(), BQ_QUERY_JOB_EXECUTION_ERROR.getErrorCode());
    }
    return returnValue;
  }

  /**
   * Method to create table in bigquery
   *
   * @param ddl ddl to create the table.
   * @return boolean status
   */
  public boolean createTableUsingDDL(String ddl, String location) {
    log.info("Received ddl for creating table:{}", ddl);
    // Create table using DDL query
    if (!StringUtils.isBlank(ddl) && queryJob(ddl, location)) {
      log.info("Table successfully got created from ddl");
    } else {
      log.error("Failed to create table from the ddl");
      throw new SnowflakeConnectorException(
          TABLE_CREATION_ERROR.getMessage(), TABLE_CREATION_ERROR.getErrorCode());
    }
    return true;
  }

  /**
   * Method to check if the table exists in BigQuery or not.
   *
   * @param bigqueryDetailsDto dto containing the required details like tablename, dataset,
   *     projectId
   * @return true if table exists, false if it does not exist.
   */
  public boolean isTableExists(BigQueryDetailsDataDTO bigqueryDetailsDto) {
    TableId tableIdObj =
        TableId.of(
            bigqueryDetailsDto.getProjectId(),
            bigqueryDetailsDto.getDatasetId(),
            bigqueryDetailsDto.getTableName());
    // Check if the table exists
    Table existingTable = bigQueryInstanceCreator.getBigQueryClient().getTable(tableIdObj);
    return existingTable != null;
  }

  /**
   * Helper method to execute the query in BigQuery
   *
   * @param sql Sql statement as string
   * @return true if this job is in JobStatus.State.DONE state or if it does not exist, false if the
   *     state is not JobStatus.State.DONE
   */
  private boolean queryJob(String sql, String location) {
    boolean jobStatus = false;

    QueryJobConfiguration queryJobConfiguration =
        QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build();
    // creating the jobId
    JobId jobId =
        JobId.newBuilder()
            .setJob("Snowflake_" + UUID.randomUUID())
            .setLocation(StringUtils.isBlank(location) ? "us" : location)
            .build();
    // Executing the query job
    try {
      Job queryJob =
          bigQueryInstanceCreator
              .getBigQueryClient()
              .create(JobInfo.newBuilder(queryJobConfiguration).setJobId(jobId).build());

      // waiting for job to finish
      queryJob = queryJob.waitFor();

      // checking if the job has any error
      if (queryJob.getStatus().getError() == null) {
        log.info("Query job executed successful for sql query:{}", sql);
        jobStatus = queryJob.isDone();
      } else {
        log.error(
            "Error query job contains error while executing. Error Message:{},\n Sql Query:{} ",
            queryJob.getStatus().getError(),
            sql);
      }
    } catch (Exception e) {
      log.error(
          "Error while executing query job. Error Message:{},\n Sql Query:{}\nStack Trace: ",
          e.getMessage(),
          sql,
          e);
      throw new SnowflakeConnectorException(
          BQ_QUERY_JOB_EXECUTION_ERROR.getMessage(), BQ_QUERY_JOB_EXECUTION_ERROR.getErrorCode());
    }
    return jobStatus;
  }
}
