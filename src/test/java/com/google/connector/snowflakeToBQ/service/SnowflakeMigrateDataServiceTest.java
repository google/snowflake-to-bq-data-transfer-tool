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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.google.connector.snowflakeToBQ.util.ErrorCode;
import com.google.connector.snowflakeToBQ.util.PropertyManager;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

public class SnowflakeMigrateDataServiceTest extends AbstractTestBase {

  @Autowired SnowflakeMigrateDataService snowflakeMigrateDataService;

  @MockBean WorkflowMigrationService workflowMigrationService;

  @MockBean SchemaExtractorService schemaExtractorService;
  @MockBean BigQueryOperationsService bigQueryJobService;

  @MockBean GoogleCloudStorageService googleCloudStorageService;

  @MockBean ApplicationConfigDataService applicationConfigDataService;

  @MockBean SnowflakesService snowflakesService;
  @MockBean SnowflakeToBQAsyncService asyncServiceTestingClass;

  @Test
  public void testMigrateDataBasic() {

    SFDataMigrationRequestDTO sfDataMigrationRequestDTO = new SFDataMigrationRequestDTO();
    sfDataMigrationRequestDTO.setSourceTableName("source_table");
    sfDataMigrationRequestDTO.setSchema(true);
    sfDataMigrationRequestDTO.setSourceSchemaName("source_public");
    sfDataMigrationRequestDTO.setSourceDatabaseName("source_database");
    sfDataMigrationRequestDTO.setLocation("us");
    sfDataMigrationRequestDTO.setGcsBucketForDDLs("gs://testing");
    sfDataMigrationRequestDTO.setGcsBucketForTranslation("gs://translation");
    sfDataMigrationRequestDTO.setSnowflakeStageLocation("gs://snowflake_stage");
    sfDataMigrationRequestDTO.setSourceDatabaseName("source_database");
    sfDataMigrationRequestDTO.setTargetSchemaName("target_schema");
    sfDataMigrationRequestDTO.setTargetDatabaseName("target_database");

    Map<String, String> tableMaps = new HashMap<>();

    tableMaps.put(
        "source_table",
        "create or replace TABLE `project`.dataset.table2 ( DATECOL DATE,DATETIMECOL"
            + " DATETIME,TIMESTAMPCOL  TIMESTAMP)");
    when(schemaExtractorService.getDDLs(any(DDLDataDTO.class))).thenReturn(tableMaps);
    when(googleCloudStorageService.moveFolder(anyString(), anyString())).thenReturn(true);
    when(applicationConfigDataService.findByColumnName(anyBoolean())).thenReturn(new ArrayList<>());

    List<GCSDetailsDataDTO> gcsDetailsDataDTOS = new ArrayList<>();

    GCSDetailsDataDTO gcsDetailsDataDTO =
        MigrateRequestMapper.getGCSDetailsDataDTOFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    gcsDetailsDataDTOS.add(gcsDetailsDataDTO);

    WorkflowMigrationResponse workflowMigrationResponse = new WorkflowMigrationResponse();
    workflowMigrationResponse.setTranslatedFileFullGCSPath("gs://testbucket/orders.sql");
    workflowMigrationResponse.setWorkflowName("TestWorkFlowName");

    when(googleCloudStorageService.writeToGCS(any(Map.class), any(GCSDetailsDataDTO.class)))
        .thenReturn(gcsDetailsDataDTOS);
    when(workflowMigrationService.createMigrationWorkflow(any(TranslateDDLDataDTO.class)))
        .thenReturn(workflowMigrationResponse);
    when(workflowMigrationService.isMigrationWorkflowCompleted(anyString())).thenReturn(true);
    ApplicationConfigData configData1 =
        MigrateRequestMapper.getApplicationConfigEntityFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    configData1.setTranslatedDDLCopied(true);
    configData1.setWorkflowName("TestWorkFlowName");
    configData1.setLastUpdatedTime(
        PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
    configData1.setRowProcessingDone(true);
    List<ApplicationConfigData> applicationConfigDataList1 = new ArrayList<>();
    applicationConfigDataList1.add(configData1);

    when(applicationConfigDataService.saveAllApplicationConfigDataServices(any(List.class)))
        .thenReturn(applicationConfigDataList1);

    when(applicationConfigDataService.findByIds(any(List.class)))
            .thenReturn(applicationConfigDataList1);
    ApplicationConfigData configData2 =
        MigrateRequestMapper.getApplicationConfigEntityFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    configData2.setDataLoadedInBQ(true);
    configData2.setDataUnloadedFromSnowflake(true);
    configData2.setBQTableCreated(true);

    CompletableFuture<OperationResult<ApplicationConfigData>> resultCompletableFuture =
        CompletableFuture.completedFuture(new OperationResult<>(configData2));
    when(asyncServiceTestingClass.snowflakeUnloadAndLoadToBQLoad(any(ApplicationConfigData.class)))
        .thenReturn(resultCompletableFuture);
    List<Long> output = snowflakeMigrateDataService.migrateData(sfDataMigrationRequestDTO);
    Assert.assertEquals(output.size(), 1);
    List<SFDataMigrationResponse> response =snowflakeMigrateDataService.getApplicationConfigDataByIds(output);
    Assert.assertTrue(response.get(0).isSuccess());
    Assert.assertFalse(response.get(0).isTableDataLoadedInBQ());
    Assert.assertFalse(response.get(0).isTableDataUnloadedFromSnowflake());
    Assert.assertTrue(response.get(0).isTableDDLTranslated());
    Assert.assertEquals(response.get(0).getSourceTableName(),"source_table");  }

  @Test
  public void testMigrateDataReturnExceptionFromAsyncMethod() {

    SFDataMigrationRequestDTO sfDataMigrationRequestDTO = new SFDataMigrationRequestDTO();
    sfDataMigrationRequestDTO.setSourceTableName("source_table");
    sfDataMigrationRequestDTO.setSchema(true);
    sfDataMigrationRequestDTO.setSourceSchemaName("source_public");
    sfDataMigrationRequestDTO.setSourceDatabaseName("source_database");
    sfDataMigrationRequestDTO.setLocation("us");
    sfDataMigrationRequestDTO.setGcsBucketForDDLs("gs://testing");
    sfDataMigrationRequestDTO.setGcsBucketForTranslation("gs://translation");
    sfDataMigrationRequestDTO.setSnowflakeStageLocation("gs://snowflake_stage");
    sfDataMigrationRequestDTO.setSourceDatabaseName("source_database");
    sfDataMigrationRequestDTO.setTargetSchemaName("target_schema");
    sfDataMigrationRequestDTO.setTargetDatabaseName("target_database");

    Map<String, String> tableMaps = new HashMap<>();

    tableMaps.put(
        "source_table",
        "create or replace TABLE `project`.dataset.table2 ( DATECOL DATE,DATETIMECOL"
            + " DATETIME,TIMESTAMPCOL  TIMESTAMP)");
    when(schemaExtractorService.getDDLs(any(DDLDataDTO.class))).thenReturn(tableMaps);
    when(googleCloudStorageService.moveFolder(anyString(), anyString())).thenReturn(true);

    // Returning null to cover the second case of condition " if (nonProcessedRows != null) ".
    when(applicationConfigDataService.findByColumnName(anyBoolean())).thenReturn(null);

    List<GCSDetailsDataDTO> gcsDetailsDataDTOS = new ArrayList<>();

    GCSDetailsDataDTO gcsDetailsDataDTO =
        MigrateRequestMapper.getGCSDetailsDataDTOFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    gcsDetailsDataDTOS.add(gcsDetailsDataDTO);

    WorkflowMigrationResponse workflowMigrationResponse = new WorkflowMigrationResponse();
    workflowMigrationResponse.setTranslatedFileFullGCSPath("gs://testbucket/orders.sql");
    workflowMigrationResponse.setWorkflowName("TestWorkFlowName");

    when(googleCloudStorageService.writeToGCS(any(Map.class), any(GCSDetailsDataDTO.class)))
        .thenReturn(gcsDetailsDataDTOS);
    when(workflowMigrationService.createMigrationWorkflow(any(TranslateDDLDataDTO.class)))
        .thenReturn(workflowMigrationResponse);
    when(workflowMigrationService.isMigrationWorkflowCompleted(anyString())).thenReturn(true);
    ApplicationConfigData configData1 =
        MigrateRequestMapper.getApplicationConfigEntityFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    configData1.setTranslatedDDLCopied(true);
    configData1.setWorkflowName("TestWorkFlowName");
    configData1.setLastUpdatedTime(
        PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
    List<ApplicationConfigData> applicationConfigDataList1 = new ArrayList<>();
    applicationConfigDataList1.add(configData1);

    when(applicationConfigDataService.saveAllApplicationConfigDataServices(any(List.class)))
        .thenReturn(applicationConfigDataList1);

    when(applicationConfigDataService.findByIds(any(List.class)))
            .thenReturn(applicationConfigDataList1);
    ApplicationConfigData configData2 =
        MigrateRequestMapper.getApplicationConfigEntityFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    configData2.setDataLoadedInBQ(true);
    configData2.setDataUnloadedFromSnowflake(true);
    configData2.setBQTableCreated(true);

    CompletableFuture<OperationResult<ApplicationConfigData>> resultCompletableFuture =
        mock(CompletableFuture.class);

    when(asyncServiceTestingClass.snowflakeUnloadAndLoadToBQLoad(any(ApplicationConfigData.class)))
        .thenReturn(resultCompletableFuture);
    doThrow(new CompletionException(new RuntimeException())).when(resultCompletableFuture).join();
    List<Long> output = snowflakeMigrateDataService.migrateData(sfDataMigrationRequestDTO);
    Assert.assertEquals(output.size(), 1);
    List<SFDataMigrationResponse> response =snowflakeMigrateDataService.getApplicationConfigDataByIds(output);
    Assert.assertFalse(response.get(0).isSuccess());
    Assert.assertFalse(response.get(0).isTableDataLoadedInBQ());
    Assert.assertFalse(response.get(0).isTableDataUnloadedFromSnowflake());
    Assert.assertTrue(response.get(0).isTableDDLTranslated());
    Assert.assertEquals(response.get(0).getSourceTableName(),"source_table");
  }

  @Test
  public void testMigrateDataReturnIsSuccessFalseFromAsyncMethod() {

    SFDataMigrationRequestDTO sfDataMigrationRequestDTO = new SFDataMigrationRequestDTO();
    sfDataMigrationRequestDTO.setSourceTableName("source_table");
    sfDataMigrationRequestDTO.setSchema(true);
    sfDataMigrationRequestDTO.setSourceSchemaName("source_public");
    sfDataMigrationRequestDTO.setSourceDatabaseName("source_database");
    sfDataMigrationRequestDTO.setLocation("us");
    sfDataMigrationRequestDTO.setGcsBucketForDDLs("gs://testing");
    sfDataMigrationRequestDTO.setGcsBucketForTranslation("gs://translation");
    sfDataMigrationRequestDTO.setSnowflakeStageLocation("gs://snowflake_stage");
    sfDataMigrationRequestDTO.setSourceDatabaseName("source_database");
    sfDataMigrationRequestDTO.setTargetSchemaName("target_schema");
    sfDataMigrationRequestDTO.setTargetDatabaseName("target_database");

    Map<String, String> tableMaps = new HashMap<>();

    tableMaps.put(
        "source_table",
        "create or replace TABLE `project`.dataset.table2 ( DATECOL DATE,DATETIMECOL"
            + " DATETIME,TIMESTAMPCOL  TIMESTAMP)");
    when(schemaExtractorService.getDDLs(any(DDLDataDTO.class))).thenReturn(tableMaps);
    when(googleCloudStorageService.moveFolder(anyString(), anyString())).thenReturn(true);
    when(applicationConfigDataService.findByColumnName(anyBoolean())).thenReturn(new ArrayList<>());

    List<GCSDetailsDataDTO> gcsDetailsDataDTOS = new ArrayList<>();

    GCSDetailsDataDTO gcsDetailsDataDTO =
        MigrateRequestMapper.getGCSDetailsDataDTOFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    gcsDetailsDataDTOS.add(gcsDetailsDataDTO);

    WorkflowMigrationResponse workflowMigrationResponse = new WorkflowMigrationResponse();
    workflowMigrationResponse.setTranslatedFileFullGCSPath("gs://testbucket/orders.sql");
    workflowMigrationResponse.setWorkflowName("TestWorkFlowName");

    when(googleCloudStorageService.writeToGCS(any(Map.class), any(GCSDetailsDataDTO.class)))
        .thenReturn(gcsDetailsDataDTOS);
    when(workflowMigrationService.createMigrationWorkflow(any(TranslateDDLDataDTO.class)))
        .thenReturn(workflowMigrationResponse);
    when(workflowMigrationService.isMigrationWorkflowCompleted(anyString())).thenReturn(true);
    ApplicationConfigData configData1 =
        MigrateRequestMapper.getApplicationConfigEntityFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    configData1.setTranslatedDDLCopied(true);
    configData1.setWorkflowName("TestWorkFlowName");
    configData1.setLastUpdatedTime(
        PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
    List<ApplicationConfigData> applicationConfigDataList1 = new ArrayList<>();
    applicationConfigDataList1.add(configData1);

    when(applicationConfigDataService.saveAllApplicationConfigDataServices(any(List.class)))
        .thenReturn(applicationConfigDataList1);
    when(applicationConfigDataService.findByIds(any(List.class)))
            .thenReturn(applicationConfigDataList1);
    ApplicationConfigData configData2 =
        MigrateRequestMapper.getApplicationConfigEntityFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    configData2.setDataLoadedInBQ(true);
    configData2.setDataUnloadedFromSnowflake(true);
    configData2.setBQTableCreated(true);
    CompletableFuture<OperationResult<ApplicationConfigData>> resultCompletableFuture =
        CompletableFuture.completedFuture(
            new OperationResult<>(
                new OperationResult.Error(
                    "Error from testMigrateDataReturnIsSuccessFalseFromAsyncMethod()")));
    when(asyncServiceTestingClass.snowflakeUnloadAndLoadToBQLoad(any(ApplicationConfigData.class)))
        .thenReturn(resultCompletableFuture);
    List<Long> output = snowflakeMigrateDataService.migrateData(sfDataMigrationRequestDTO);
    Assert.assertEquals(output.size(), 1);
    List<SFDataMigrationResponse> response =snowflakeMigrateDataService.getApplicationConfigDataByIds(output);
    Assert.assertFalse(response.get(0).isSuccess());
    Assert.assertFalse(response.get(0).isTableDataLoadedInBQ());
    Assert.assertFalse(response.get(0).isTableDataUnloadedFromSnowflake());
    Assert.assertTrue(response.get(0).isTableDDLTranslated());
    Assert.assertEquals(response.get(0).getSourceTableName(),"source_table");
  }

  @Test
  public void testWorkflowCouldNotFinishInTime() {

    SFDataMigrationRequestDTO sfDataMigrationRequestDTO = new SFDataMigrationRequestDTO();
    sfDataMigrationRequestDTO.setSourceTableName("source_table");
    sfDataMigrationRequestDTO.setSchema(true);
    sfDataMigrationRequestDTO.setSourceSchemaName("source_public");
    sfDataMigrationRequestDTO.setSourceDatabaseName("source_database");
    sfDataMigrationRequestDTO.setLocation("us");
    sfDataMigrationRequestDTO.setGcsBucketForDDLs("gs://testing");
    sfDataMigrationRequestDTO.setGcsBucketForTranslation("gs://translation");
    sfDataMigrationRequestDTO.setSnowflakeStageLocation("gs://snowflake_stage");
    sfDataMigrationRequestDTO.setSourceDatabaseName("source_database");
    sfDataMigrationRequestDTO.setTargetSchemaName("target_schema");
    sfDataMigrationRequestDTO.setTargetDatabaseName("target_database");

    Map<String, String> tableMaps = new HashMap<>();

    tableMaps.put(
        "source_table",
        "create or replace TABLE `project`.dataset.table2 ( DATECOL DATE,DATETIMECOL"
            + " DATETIME,TIMESTAMPCOL  TIMESTAMP)");
    when(schemaExtractorService.getDDLs(any(DDLDataDTO.class))).thenReturn(tableMaps);
    when(googleCloudStorageService.moveFolder(anyString(), anyString())).thenReturn(true);
    when(applicationConfigDataService.findByColumnName(anyBoolean())).thenReturn(new ArrayList<>());

    List<GCSDetailsDataDTO> gcsDetailsDataDTOS = new ArrayList<>();

    GCSDetailsDataDTO gcsDetailsDataDTO =
        MigrateRequestMapper.getGCSDetailsDataDTOFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    gcsDetailsDataDTOS.add(gcsDetailsDataDTO);

    WorkflowMigrationResponse workflowMigrationResponse = new WorkflowMigrationResponse();
    workflowMigrationResponse.setTranslatedFileFullGCSPath("gs://testbucket/orders.sql");
    workflowMigrationResponse.setWorkflowName("TestWorkFlowName");

    when(googleCloudStorageService.writeToGCS(any(Map.class), any(GCSDetailsDataDTO.class)))
        .thenReturn(gcsDetailsDataDTOS);
    when(workflowMigrationService.createMigrationWorkflow(any(TranslateDDLDataDTO.class)))
        .thenReturn(workflowMigrationResponse);
    when(workflowMigrationService.isMigrationWorkflowCompleted(anyString())).thenReturn(false);
    try {
      snowflakeMigrateDataService.migrateData(sfDataMigrationRequestDTO);
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(
          e.getMessage(),
          ErrorCode.MIGRATION_WORKFLOW_EXECUTION_ERROR.getMessage()
              + " Could not finish with in given time");
      Assert.assertEquals(
          e.getErrorCode(), ErrorCode.MIGRATION_WORKFLOW_EXECUTION_ERROR.getErrorCode());
    }
  }
}
