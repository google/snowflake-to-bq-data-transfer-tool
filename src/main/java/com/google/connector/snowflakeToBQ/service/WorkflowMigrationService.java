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

import static com.google.connector.snowflakeToBQ.util.ErrorCode.MIGRATION_WORKFLOW_EXECUTION_ERROR;
import static com.google.connector.snowflakeToBQ.util.PropertyManager.TRANSLATION_TYPE;

import com.google.cloud.bigquery.migration.v2.*;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.TranslateDDLDataDTO;
import com.google.connector.snowflakeToBQ.model.response.WorkflowMigrationResponse;
import com.google.connector.snowflakeToBQ.service.Instancecreator.MigrationServiceInstanceCreator;
import com.google.connector.snowflakeToBQ.util.PropertyManager;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import net.snowflake.client.jdbc.internal.apache.tika.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** Class to help in executing the migration workflow(translation of query) */
@Service
public class WorkflowMigrationService {

  private static final Logger log = LoggerFactory.getLogger(WorkflowMigrationService.class);

  @Value("${migration.workflow.duration}")
  private int migratingWorkflowDuration;

  private final MigrationServiceInstanceCreator migrationServiceInstanceCreator;

  public WorkflowMigrationService(MigrationServiceInstanceCreator migrationServiceInstanceCreator) {
    this.migrationServiceInstanceCreator = migrationServiceInstanceCreator;
  }

  /**
   * Method to create new workflow migration
   *
   * @param translateDDLDataDTO required parameter for creating the migration workflow.
   * @return workflow name
   */
  public WorkflowMigrationResponse createMigrationWorkflow(
      TranslateDDLDataDTO translateDDLDataDTO) {

    log.info("Creating the migration flow for request:{}", translateDDLDataDTO.toString());

    WorkflowMigrationResponse workflowMigrationResponse;

    try {
      MigrationWorkflow response =
          migrationServiceInstanceCreator
              .getMigrationServiceClient()
              .createMigrationWorkflow(buildCreateMigrationWorkflowRequest(translateDDLDataDTO));
      workflowMigrationResponse = new WorkflowMigrationResponse();
      workflowMigrationResponse.setWorkflowName(response.getName());
      workflowMigrationResponse.setTranslatedFileFullGCSPath(
          getOutputFolderForTranslation(translateDDLDataDTO));
    } catch (Exception e) {
      log.error("Error while creating the executing the migration workflow:{}", e.getMessage());
      throw new SnowflakeConnectorException(
          MIGRATION_WORKFLOW_EXECUTION_ERROR.getMessage(),
          MIGRATION_WORKFLOW_EXECUTION_ERROR.getErrorCode());
    }
    return workflowMigrationResponse;
  }

  /**
   * Verify if the workflow is completed or not. It run a while loop with max period of 2 min. Loop
   * pauses 5 sec after each execution. If workflow does not finish in 2 min it returns false as an
   * output
   *
   * @param migrationWorkflowName created workflow name whose status is to be checked.
   * @return true if workflow completes with in the max time defined in while loop, otherwise false.
   */
  public boolean isMigrationWorkflowCompleted(String migrationWorkflowName) {
    boolean returnValue = false;
    long startTime = System.currentTimeMillis();

    migratingWorkflowDuration = migratingWorkflowDuration != 0 ? migratingWorkflowDuration : 120000;

    log.info(
        "Waiting for Migration Workflow to complete, will wait for {} Sec before terminating the"
            + " process",
        migratingWorkflowDuration);
    while (System.currentTimeMillis() - startTime < migratingWorkflowDuration) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        log.error(
            "Thread.sleep got interrupted while sleeping in Migration Workflow to check the status"
                + " of migration flow");
        throw new SnowflakeConnectorException(
            MIGRATION_WORKFLOW_EXECUTION_ERROR.getMessage(),
            MIGRATION_WORKFLOW_EXECUTION_ERROR.getErrorCode());
      }
      MigrationWorkflow response =
          migrationServiceInstanceCreator
              .getMigrationServiceClient()
              .getMigrationWorkflow(migrationWorkflowName);

      if (response.getState()
          == com.google.cloud.bigquery.migration.v2.MigrationWorkflow.State.COMPLETED) {
        returnValue = true;
        log.info(
            "Migration Workflow response state:{}, return value :{}",
            response.getState(),
            returnValue);
        break;
      } else if (response.getState()
          == com.google.cloud.bigquery.migration.v2.MigrationWorkflow.State.PAUSED) {
        log.info(
            "Migration Workflow  response state:{}, return value :{}",
            response.getState(),
            returnValue);
        break;
      }
    }
    return returnValue;
  }

  /**
   * Build workflow request based on the input parameters received from caller.
   *
   * @param translateDDLDataDTO required parameter for creating the workflow.
   * @return @{@link CreateMigrationWorkflowRequest}
   */
  private CreateMigrationWorkflowRequest buildCreateMigrationWorkflowRequest(
      TranslateDDLDataDTO translateDDLDataDTO) {
    Dialect bigqueryDialect =
        Dialect.newBuilder().setBigqueryDialect(BigQueryDialect.getDefaultInstance()).build();
    Dialect snowflakeDialect =
        Dialect.newBuilder().setSnowflakeDialect(SnowflakeDialect.getDefaultInstance()).build();
    List<String> schemaList = new ArrayList<>();
    schemaList.add(translateDDLDataDTO.getSourceSchemaName());
    // Setting the source environment
    SourceEnv sourceEnv =
        SourceEnv.newBuilder()
            .setDefaultDatabase(translateDDLDataDTO.getSourceDatabaseName())
            .addAllSchemaSearchPath(schemaList)
            .build();
    // Creating the source to target schema mapping, this will help replacing the source database
    // name with target projectId etc.
    ObjectNameMapping m =
        ObjectNameMapping.newBuilder()
            .setSource(
                NameMappingKey.newBuilder()
                    .setDatabase(translateDDLDataDTO.getSourceDatabaseName())
                    .setSchema(translateDDLDataDTO.getSourceSchemaName())
                    .build())
            .setTarget(
                NameMappingValue.newBuilder()
                    .setDatabase(translateDDLDataDTO.getTargetDatabaseName())
                    .setSchema(translateDDLDataDTO.getTargetSchemaName())
                    .build())
            .build();
    ObjectNameMappingList l = ObjectNameMappingList.newBuilder().addNameMap(m).build();

    TranslationConfigDetails translationConfigDetails =
        TranslationConfigDetails.newBuilder()
            .setRequestSource("Snowflake-to-BQ")
            .setGcsSourcePath(
                String.format(
                    "gs://%s/%s",
                    translateDDLDataDTO.getGcsBucketForTranslation(),
                    getInputFolderForTranslation(translateDDLDataDTO)))
            .setGcsTargetPath(
                String.format(
                    "gs://%s/%s",
                    translateDDLDataDTO.getGcsBucketForTranslation(),
                    getOutputFolderForTranslation(translateDDLDataDTO)))
            .setSourceDialect(snowflakeDialect)
            .setTargetDialect(bigqueryDialect)
            .setSourceEnv(sourceEnv)
            .setNameMappingList(l)
            .build();

    return CreateMigrationWorkflowRequest.newBuilder()
        .setParent(
            String.format(
                "projects/%s/locations/%s",
                translateDDLDataDTO.getTargetDatabaseName(),
                StringUtils.isBlank(translateDDLDataDTO.getTranslationJobLocation())
                    ? "us"
                    : translateDDLDataDTO.getTranslationJobLocation()))
        .setMigrationWorkflow(
            MigrationWorkflow.newBuilder()
                .setDisplayName(
                    TRANSLATION_TYPE
                        + "-"
                        + ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT))
                .putTasks(
                    "translation-task",
                    MigrationTask.newBuilder()
                        .setType(TRANSLATION_TYPE)
                        .setTranslationConfigDetails(translationConfigDetails)
                        .build())
                .build())
        .build();
  }

  private String getInputFolderForTranslation(TranslateDDLDataDTO translateDDLDataDTO) {
    return String.format(
        "%s/%s/%s",
        PropertyManager.DDL_PREFIX,
        translateDDLDataDTO.getSourceDatabaseName(),
        translateDDLDataDTO.getSourceSchemaName());
  }

  private String getOutputFolderForTranslation(TranslateDDLDataDTO translateDDLDataDTO) {
    return String.format(
        "%s/%s/%s/%s",
        PropertyManager.SNOWFLAKE_TRANSLATED_FOLDER_PREFIX,
        translateDDLDataDTO.getSourceDatabaseName(),
        translateDDLDataDTO.getSourceSchemaName(),
        PropertyManager.getDateInDesiredFormat(
            LocalDateTime.now(), PropertyManager.OUTPUT_FORMATTER));
  }
}
