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

package com.google.connector.snowflakeToBQ.service.async;

import static com.google.connector.snowflakeToBQ.util.ErrorCode.TABLE_ALREADY_EXISTS;
import static com.google.connector.snowflakeToBQ.util.PropertyManager.OUTPUT_FORMATTER1;

import com.google.connector.snowflakeToBQ.entity.ApplicationConfigData;
import com.google.connector.snowflakeToBQ.mapper.MigrateRequestMapper;
import com.google.connector.snowflakeToBQ.model.OperationResult;
import com.google.connector.snowflakeToBQ.model.datadto.BigQueryDetailsDataDTO;
import com.google.connector.snowflakeToBQ.service.ApplicationConfigDataService;
import com.google.connector.snowflakeToBQ.service.BigQueryOperationsService;
import com.google.connector.snowflakeToBQ.service.GoogleCloudStorageService;
import com.google.connector.snowflakeToBQ.service.SnowflakesService;
import com.google.connector.snowflakeToBQ.util.PropertyManager;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * Class to perform Snowflake data unload and bigquery load operation in Asynchronous way. While
 * these two operations occur sequentially, other requests carrying out the same tasks will run in
 * parallel with each other, depending on the Thread Executor settings. This is using the spring
 * boot Async feature.
 */
@Service
public class SnowflakeToBQAsyncService {
  private static final Logger log = LoggerFactory.getLogger(SnowflakeToBQAsyncService.class);

  final BigQueryOperationsService bigQueryOperationsService;

  final SnowflakesService snowflakesService;

  final ApplicationConfigDataService applicationConfigDataService;

  final GoogleCloudStorageService googleCloudStorageService;

  public SnowflakeToBQAsyncService(
      BigQueryOperationsService bigQueryOperationsService,
      SnowflakesService snowflakesService,
      ApplicationConfigDataService applicationConfigDataService,
      GoogleCloudStorageService googleCloudStorageService) {
    this.bigQueryOperationsService = bigQueryOperationsService;
    this.snowflakesService = snowflakesService;
    this.applicationConfigDataService = applicationConfigDataService;
    this.googleCloudStorageService = googleCloudStorageService;
  }

  /**
   * Method to perform the Snowflake table data unloading to GCS and loading the data in BigQuery in
   * Asynchronous way. Here "customExecutor" annotation is used which defined the Thread executor
   * and its property.
   *
   * @param applicationConfigData Application data
   * @return @{@link CompletableFuture} result of the execution, success or fail
   */
  @Async("customExecutor")
  public CompletableFuture<OperationResult<ApplicationConfigData>> snowflakeUnloadAndLoadToBQLoad(
      ApplicationConfigData applicationConfigData) {

    // We are using Aysnc method here so the method which is calling it can not pass its context to
    // this method directly and requestId was coming as null which was set in controller. Hence we
    // have saved it in the application data and used it to set again in this method in MDC. This
    // way any following method call from this method will log that entry, and we will get same
    // requestId logged from Controller till the end of all the calls.

    MDC.put("requestLogId", applicationConfigData.getRequestLogId());

    log.info("Inside SnowflakeUnloadToBQLoad() of SnowflakeToBQAsyncService class");

    BigQueryDetailsDataDTO bigQueryDetailsDataDTO =
        MigrateRequestMapper.migrateRequestToBigQueryDetailDataDto(applicationConfigData);
    log.info(
        "The value received for applicationConfigData.isBQTableCreated() property is ::{}",
        applicationConfigData.isBQTableCreated());
    // This property is serve two purposes, 1: It checks if during the execution this step completed
    // or not. This value gets updated in database if below steps finishes properly. 2: It takes
    // this value from client request to check if table created should be skipped or not. Client
    // request value takes preference  over database value so if client sends false and database
    // value is true then false will take preference.
    if (!applicationConfigData.isBQTableCreated()) {

      // Checking if table already existing and user request to create it based on above if
      // condition
      boolean isBQTableExistsTemp = bigQueryOperationsService.isTableExists(bigQueryDetailsDataDTO);

      log.info(
          "Table Name::{}, in Project::{} and Dataset::{}, exists ::{}",
          bigQueryDetailsDataDTO.getTableName(),
          bigQueryDetailsDataDTO.getProjectId(),
          bigQueryDetailsDataDTO.getDatasetId(),
          isBQTableExistsTemp);

      // Error will be thrown becasue tables existing although user has request to create a new
      // table. As per design code will never drop or recreate if it table exists
      if (isBQTableExistsTemp) {
        log.error(
            "{},Error Code:{}, table name:{}",
            TABLE_ALREADY_EXISTS.getMessage(),
            TABLE_ALREADY_EXISTS.getErrorCode(),
            bigQueryDetailsDataDTO.getTableName());

        applicationConfigData.setLastUpdatedTime(
            PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
        applicationConfigDataService.saveApplicationConfigDataService(applicationConfigData);

        return CompletableFuture.completedFuture(
            new OperationResult<>(
                new OperationResult.Error(
                    String.format(
                        "%s, %s, Error Code:%s",
                        bigQueryDetailsDataDTO.getTableName(),
                        TABLE_ALREADY_EXISTS.getMessage(),
                        TABLE_ALREADY_EXISTS.getErrorCode()))));
      }

      // Update the DDl content
      String updatedDDL = updateDDLContent(applicationConfigData);
      log.info("Translated ddl after updating the database, schema and tablename::{}", updatedDDL);

      // Creating a table based on the updated DDL
      boolean tableCreated =
          bigQueryOperationsService.createTableUsingDDL(
              updatedDDL, bigQueryDetailsDataDTO.getLocation());
      log.info("createTableUsingDDL() returned value is ::{}", tableCreated);
      // Marking the step complete for the row
      applicationConfigData.setBQTableCreated(true);
      applicationConfigData.setLastUpdatedTime(
          PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
      applicationConfigDataService.saveApplicationConfigDataService(applicationConfigData);
    }

    // Checking if this step is already completed
    if (!applicationConfigData.isDataUnloadedFromSnowflake()) {
      // Starting the execution of Data unload from Snowflake using rest API.
      String snowflakeStatementHandle =
          snowflakesService.executeUnloadDataCommand(
              applicationConfigData.getTargetTableName(),
              applicationConfigData.getSnowflakeStageLocation(),
              applicationConfigData.getSnowflakeFileFormatValue());
      log.info(
          "Snowflake statement handle:: {}, for table name:: {}",
          snowflakeStatementHandle,
          applicationConfigData.getTargetTableName());
      // Marking the step complete
      applicationConfigData.setDataUnloadedFromSnowflake(true);
      applicationConfigData.setSnowflakeStatementHandle(snowflakeStatementHandle);
      applicationConfigData.setLastUpdatedTime(
          PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
      applicationConfigDataService.saveApplicationConfigDataService(applicationConfigData);
    }

    // Checking if this step is already completed
    if (!applicationConfigData.isDataLoadedInBQ()) {
      try {
        bigQueryOperationsService.loadBigQueryJob(bigQueryDetailsDataDTO);
      } catch (Exception e) {
        log.error(
            "Error while loading data in the table:{} ",
            applicationConfigData.getTargetTableName());

        applicationConfigData.setLastUpdatedTime(
            PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
        applicationConfigDataService.saveApplicationConfigDataService(applicationConfigData);

        return CompletableFuture.completedFuture(
            new OperationResult<>(
                new OperationResult.Error(
                    String.format(
                        "%s, Error:%s",
                        applicationConfigData.getTargetTableName(), e.getMessage()))));
      }
      applicationConfigData.setDataLoadedInBQ(true);
      applicationConfigData.setLastUpdatedTime(
          PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
      applicationConfigDataService.saveApplicationConfigDataService(applicationConfigData);
    }
    MDC.remove("requestLogId");
    return CompletableFuture.completedFuture(new OperationResult<>(applicationConfigData));
  }

  /**
   * This method is needed because migration API name mapping is not working. It seems they have
   * updated the API behind the scene. here Its updated the source Database, Schema and table name
   * with target values
   *
   * @param applicationConfigData Data related to migrate request
   * @return updated ddl
   */
  private String updateDDLContent(ApplicationConfigData applicationConfigData) {
    String ddlContent =
        googleCloudStorageService.getContentFromGCSFile(
            applicationConfigData.getGcsBucketForTranslation(),
            applicationConfigData.getTranslatedDDLGCSPath());
    // Update the source database with target
    String updatedContent =
        StringUtils.replaceIgnoreCase(
            ddlContent,
            applicationConfigData.getSourceDatabaseName(),
            applicationConfigData.getTargetDatabaseName());
    // Update the source schema with target
    updatedContent =
        StringUtils.replaceIgnoreCase(
            updatedContent,
            applicationConfigData.getSourceSchemaName(),
            applicationConfigData.getTargetSchemaName());
    // Update the source table name with target
    return StringUtils.replaceIgnoreCase(
        updatedContent,
        applicationConfigData.getSourceTableName(),
        applicationConfigData.getTargetTableName());
  }
}
