/*
 * Copyright 2023 Google LLC
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

import static com.google.connector.snowflakeToBQ.util.PropertyManager.OUTPUT_FORMATTER1;

import com.google.connector.snowflakeToBQ.entity.ApplicationConfigData;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.mapper.MigrateRequestMapper;
import com.google.connector.snowflakeToBQ.model.OperationResult;
import com.google.connector.snowflakeToBQ.model.datadto.DDLDataDTO;
import com.google.connector.snowflakeToBQ.model.datadto.GCSDetailsDataDTO;
import com.google.connector.snowflakeToBQ.model.datadto.TranslateDDLDataDTO;
import com.google.connector.snowflakeToBQ.model.request.SFDataMigrationRequestDTO;
import com.google.connector.snowflakeToBQ.model.response.SFDataMigrationResponse;
import com.google.connector.snowflakeToBQ.model.response.WorkflowMigrationResponse;
import com.google.connector.snowflakeToBQ.service.async.SnowflakeToBQAsyncService;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import com.google.connector.snowflakeToBQ.util.PropertyManager;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

/**
 * Class to perform the migration of table data from Snowflake to Bigquery. This is kind of manager
 * class which calls different service classes to perform different options. Each calls individual
 * service and pass the result to next service(if required).
 */
@Service
public class SnowflakeMigrateDataService {
  private static final Logger log = LoggerFactory.getLogger(SnowflakeMigrateDataService.class);
  final WorkflowMigrationService workflowMigrationService;

  final SchemaExtractorService schemaExtractorService;
  final BigQueryOperationsService bigQueryJobService;

  final GoogleCloudStorageService googleCloudStorageService;

  final BigQueryOperationsService loadBigQueryJobService;
  final ApplicationConfigDataService applicationConfigDataService;

  final SnowflakesService snowflakesService;
  final SnowflakeToBQAsyncService snowflakeToBQAsyncService;

  public SnowflakeMigrateDataService(
      WorkflowMigrationService workflowMigrationService,
      SchemaExtractorService schemaExtractorService,
      BigQueryOperationsService bigQueryJobService,
      GoogleCloudStorageService googleCloudStorageService,
      BigQueryOperationsService loadBigQueryJobService,
      ApplicationConfigDataService applicationConfigDataService,
      SnowflakesService snowflakesService,
      SnowflakeToBQAsyncService snowflakeToBQAsyncService) {
    this.workflowMigrationService = workflowMigrationService;
    this.schemaExtractorService = schemaExtractorService;
    this.bigQueryJobService = bigQueryJobService;
    this.googleCloudStorageService = googleCloudStorageService;
    this.loadBigQueryJobService = loadBigQueryJobService;
    this.applicationConfigDataService = applicationConfigDataService;
    this.snowflakesService = snowflakesService;
    this.snowflakeToBQAsyncService = snowflakeToBQAsyncService;
  }

  /**
   * Method which helps in completing the Snowflake to BigQuery table data migration.
   *
   * @param sfDataMigrationRequestDTO DTO object which contains the required data for performing the
   *     Migration.
   * @return List of Ids created and executed during this method call.
   */
  public List<Long> migrateData(SFDataMigrationRequestDTO sfDataMigrationRequestDTO) {
    List<Long> requestIds = null;

    // Extracting ddlDataDTO fields from the received input request.
    try {
      DDLDataDTO ddlDataDTO =
          MigrateRequestMapper.getDDLDataDTOFromSFDataMigrationRequestDTO(
              sfDataMigrationRequestDTO);

      // Extracting all the DDLS for the tables which are received as a part of input request.
      Map<String, String> ddls = schemaExtractorService.getDDLs(ddlDataDTO);

      // Extracting GCSDetailsDataDTO from the received input request
      GCSDetailsDataDTO gcsDetailsDataDTO =
          MigrateRequestMapper.getGCSDetailsDataDTOFromSFDataMigrationRequestDTO(
              sfDataMigrationRequestDTO);
      // Writing ddls to GCS bucket
      List<GCSDetailsDataDTO> gcsDetailsDataDTOS =
          googleCloudStorageService.writeToGCS(ddls, gcsDetailsDataDTO);

      // Retrieving the already save rows where processing is not done and new rows which are based
      // on
      // received Input.
      List<ApplicationConfigData> applicationConfigDataList =
          saveAllTheRequestsToDatabase(sfDataMigrationRequestDTO, gcsDetailsDataDTOS);

      requestIds = getCurrentlyProcessingRequestIdsFromDatabase(applicationConfigDataList);

      // This condition is included to address a situation in which a user creates a table
      // independently, without relying on the DDL (Data Definition Language) and translation
      // processes of the connector. In such a scenario, translation is not necessary for table
      // creation. It also resolves another use case where translation is not supported by BQMS
      // (BigQuery Migration Service). In this case, if a user creates the table, the application
      // will generate an error because it won't be able to perform the translation. To prevent this
      // scenario, this condition is added.While this condition could have been applied when
      // extracting the DDL and writing it to the GCS (Google Cloud Storage), it's not used there.
      // This is because, for all tables, the rows in the database are saved after writing the DDL
      // to the GCS bucket. Therefore, this step is required even when translation is not needed.

      if (!sfDataMigrationRequestDTO.isBqTableExists()) {
        // Extracting TranslateDDLDataDTO from the received input request
        TranslateDDLDataDTO translateDDLDataDTO =
            MigrateRequestMapper.getTranslateDDLDataDTOFromSFDataMigrationRequestDTO(
                sfDataMigrationRequestDTO);

        // Translating the ddls using migration workflow service
        applicationConfigDataList =
            executeDDLTranslation(translateDDLDataDTO, applicationConfigDataList);
      }
      commonCodeToExecuteApplicationConfigDataForMigration(applicationConfigDataList);
    } catch (Exception e) {
      log.error("Error while performing SnowflakeMigrationData request,{}", e.getMessage());
    }
    return requestIds;
  }

  /**
   * Method to read all the rows of {@link ApplicationConfigData} from table based on the promiary
   * key id values
   *
   * @param requestIds Primary key of the @{@link ApplicationConfigData} table, requestIDs from
   *     application perspective.
   * @return List of {@link SFDataMigrationResponse} which is mapped from {@link
   *     ApplicationConfigData} for the received input.
   */
  public List<SFDataMigrationResponse> getApplicationConfigDataByIds(List<Long> requestIds) {
    List<ApplicationConfigData> applicationConfigDataList =
        applicationConfigDataService.findByIds(requestIds);
    List<SFDataMigrationResponse> sfDataMigrationResponses = new ArrayList<>();
    for (ApplicationConfigData tempApplicationConfigData : applicationConfigDataList) {

      SFDataMigrationResponse sfDataMigrationResponse =
          MigrateRequestMapper.applicationConfigDataToSFSfDataMigrationResponse(
              tempApplicationConfigData);
      sfDataMigrationResponses.add(sfDataMigrationResponse);
    }
    return sfDataMigrationResponses;
  }

  /*
   * Method to receive requests to process failed requests. This application stores the request data
   * in an embedded database. The request data contains all the data related to the table for
   * migration. This application stores data for different stages like DDL extraction, translation,
   * Snowflake unload, etc. This step will process the failed requests which have passed the
   * Snowflake unload step. This means that if the extraction DDL, translation DDL and Snowflake
   * unload are successfully executed for any request, but it failed while executing the next steps,
   * then this method can be used to reprocess those requests.
   *
   * @return List of Ids created and executed during this method call.
   */
  public List<Long> processFailedRequestMigratedRows() {
    List<Long> requestIds = null;

    // Extracting ddlDataDTO fields from the received input request.
    try {
      // Getting the rows from table which are unprocessed due to any reason, in table it should be
      // defined by is_processing_done
      List<ApplicationConfigData> applicationConfigDataList =
          applicationConfigDataService.findByColumnName(false);

      if (applicationConfigDataList.isEmpty()) {
        return Collections.emptyList();
      }

      List<ApplicationConfigData> eligibleFailedRequestsForProcessing =
          getFailedRequestEligibleForProcessing(applicationConfigDataList);

      if (eligibleFailedRequestsForProcessing.isEmpty()) {
        return requestIds;
      }
      requestIds =
          getCurrentlyProcessingRequestIdsFromDatabase(eligibleFailedRequestsForProcessing);

      commonCodeToExecuteApplicationConfigDataForMigration(applicationConfigDataList);
    } catch (Exception e) {
      log.error(
          "Error while performing SnowflakeMigrationData for the failed request of the table,{}",
          e.getMessage());
    }
    return requestIds;
  }

  /**
   * This method is created to avoid duplicate code from two caller method which are performing the
   * similar task. This method takes the applicationConfigData (which is a data related to a table
   * for migrations) and perform Snowflake Unload, Table creation if needed and load data to the
   * table.
   *
   * @param applicationConfigDataList {@link List} of {@link ApplicationConfigData} which is a data
   *     related to table to be migrated.
   */
  private void commonCodeToExecuteApplicationConfigDataForMigration(
      List<ApplicationConfigData> applicationConfigDataList) {

    List<CompletableFuture<OperationResult<ApplicationConfigData>>> asyncFutureResultList =
        new ArrayList<>();

    // Executing all the request for each table in parallel(Async)
    for (ApplicationConfigData applicationConfigDataTemp : applicationConfigDataList) {
      CompletableFuture<OperationResult<ApplicationConfigData>> asyncFutureResult =
          snowflakeToBQAsyncService.snowflakeUnloadAndLoadToBQLoad(applicationConfigDataTemp);
      asyncFutureResultList.add(asyncFutureResult);
    }

    // Fetching the result of each table request. Below code will wait to fetch the
    // result of each request and make sure that we send response to user once everything is done.
    for (CompletableFuture<OperationResult<ApplicationConfigData>> tempAsyncFutureResult :
        asyncFutureResultList) {

      try {

        OperationResult<ApplicationConfigData> tempOperationResult = tempAsyncFutureResult.join();
        // Once we are successful in fetching the status for table request, it's processing status
        // will be set as true to avoid rerunning the row in next iteration
        if (tempOperationResult.isSuccess()) {
          // Setting the processing of the row to done
          tempOperationResult.getResult().setRowProcessingDone(true);
          tempOperationResult
              .getResult()
              .setLastUpdatedTime(
                  PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
          applicationConfigDataService.saveApplicationConfigDataService(
              tempOperationResult.getResult());
        } else {
          log.error("Error:Snowflake Unload and load to BQ");
        }
      } catch (CompletionException e) {
        log.error(
            "Exception while processing the Snowflake unload and BQ load request, error message:{}",
            e.getMessage());
      }
    }
  }

  /**
   * Helper method to perform the translation of the extracted DDLS for the Snowflake tables. It
   * also updates the {@link ApplicationConfigData} table after finishing the translation step
   *
   * @param translateDDLDataDTO DTO which contain values required for performing the translation
   * @param allApplicationDatas All the rows which are present in the table for processing
   * @return List of updated {@link ApplicationConfigData}
   */
  private List<ApplicationConfigData> executeDDLTranslation(
      TranslateDDLDataDTO translateDDLDataDTO, List<ApplicationConfigData> allApplicationDatas) {

    WorkflowMigrationResponse workflowMigrationResponse =
        workflowMigrationService.createMigrationWorkflow(translateDDLDataDTO);

    boolean returnValue =
        workflowMigrationService.isMigrationWorkflowCompleted(
            workflowMigrationResponse.getWorkflowName());

    if (!returnValue) {
      throw new SnowflakeConnectorException(
          ErrorCode.MIGRATION_WORKFLOW_EXECUTION_ERROR.getMessage()
              + " Could not finish with in given time",
          ErrorCode.MIGRATION_WORKFLOW_EXECUTION_ERROR.getErrorCode());
    }

    for (ApplicationConfigData applicationConfigDataTemp : allApplicationDatas) {
      applicationConfigDataTemp.setWorkflowName(workflowMigrationResponse.getWorkflowName());
      // Updating the translation path to be saved in table.
      applicationConfigDataTemp.setTranslatedDDLGCSPath(
          String.format(
              "%s/%s.sql",
              workflowMigrationResponse.getTranslatedFileFullGCSPath(),
              applicationConfigDataTemp.getTargetTableName()));
      applicationConfigDataTemp.setTranslatedDDLCopied(true);
      applicationConfigDataTemp.setLastUpdatedTime(
          PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
    }
    allApplicationDatas =
        applicationConfigDataService.saveAllApplicationConfigDataServices(allApplicationDatas);
    return allApplicationDatas;
  }

  /** Helper method to update Application data table for GCS related data. */
  private List<ApplicationConfigData> saveAllTheRequestsToDatabase(
      SFDataMigrationRequestDTO sfDataMigrationRequestDTO,
      List<GCSDetailsDataDTO> gcsDetailsDataDTOS) {

    List<ApplicationConfigData> applicationConfigDataList = new ArrayList<>();
    ApplicationConfigData applicationConfigData =
        MigrateRequestMapper.getApplicationConfigEntityFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    for (GCSDetailsDataDTO gcsDetailsDataDTOTemp : gcsDetailsDataDTOS) {
      ApplicationConfigData applicationConfigDataTemp =
          SerializationUtils.clone(applicationConfigData);

      applicationConfigDataTemp.setSourceTableName(gcsDetailsDataDTOTemp.getSourceTableName());
      applicationConfigDataTemp.setTargetTableName(gcsDetailsDataDTOTemp.getSourceTableName());
      applicationConfigDataTemp.setSourceDDLCopied(gcsDetailsDataDTOTemp.isSourceDDLCopied());
      applicationConfigDataTemp.setCreatedTime(
          PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
      applicationConfigDataTemp.setLastUpdatedTime(
          PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
      applicationConfigDataList.add(applicationConfigDataTemp);
    }
    return applicationConfigDataService.saveAllApplicationConfigDataServices(
        applicationConfigDataList);
  }

  private List<Long> getCurrentlyProcessingRequestIdsFromDatabase(
      List<ApplicationConfigData> applicationConfigDataList) {
    List<Long> requestIds = new ArrayList<>();
    for (ApplicationConfigData tempApplicationConfigData : applicationConfigDataList) {
      requestIds.add(tempApplicationConfigData.getId());
    }
    return requestIds;
  }

  /**
   * This method give the filters the {@link ApplicationConfigData} list based on the
   * isTranslatedDDLCopied flag. It returns the list where isDataUnloadedFromSnowflake=true, means
   * the snowflake unload table command for those feeds are done.
   *
   * @param applicationConfigDataList {@link List} of {@link ApplicationConfigData}
   * @return {@link List} of {@link ApplicationConfigData} where isTranslatedDDLCopied=true
   */
  private List<ApplicationConfigData> getFailedRequestEligibleForProcessing(
      List<ApplicationConfigData> applicationConfigDataList) {
    List<ApplicationConfigData> eligibleRequestReprocessingList = new ArrayList<>();
    for (ApplicationConfigData tempApplicationConfigData : applicationConfigDataList) {

      if (tempApplicationConfigData.isDataUnloadedFromSnowflake()) {
        tempApplicationConfigData.setLastUpdatedTime(
            PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
        tempApplicationConfigData.setRequestLogId(MDC.get("requestLogId"));
        eligibleRequestReprocessingList.add(tempApplicationConfigData);
      }
    }
    return eligibleRequestReprocessingList;
  }
}
