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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.google.cloud.bigquery.migration.v2.CreateMigrationWorkflowRequest;
import com.google.cloud.bigquery.migration.v2.MigrationServiceClient;
import com.google.cloud.bigquery.migration.v2.MigrationWorkflow;
import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.TranslateDDLDataDTO;
import com.google.connector.snowflakeToBQ.model.response.WorkflowMigrationResponse;
import com.google.connector.snowflakeToBQ.service.Instancecreator.MigrationServiceInstanceCreator;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

public class WorkflowMigrationServiceTest extends AbstractTestBase {

  @Autowired
  WorkflowMigrationService workflowMigrationService;

  @MockBean MigrationServiceInstanceCreator migrationServiceInstanceCreator;

  @Test
  public void testMigrationWorkflowBasic() {

    TranslateDDLDataDTO translateDDLDataDTO = new TranslateDDLDataDTO();
    translateDDLDataDTO.setSourceSchemaName("source_public");
    translateDDLDataDTO.setSourceDatabaseName("source_database");
    translateDDLDataDTO.setTranslationJobLocation("us");
    translateDDLDataDTO.setGcsBucketForTranslation("gs://translation");
    translateDDLDataDTO.setSourceDatabaseName("source_database");
    translateDDLDataDTO.setTargetSchemaName("target_schema");
    translateDDLDataDTO.setTargetDatabaseName("target_database");

    MigrationServiceClient migrationServiceClientMock = mock(MigrationServiceClient.class);
    MigrationWorkflow response =
        MigrationWorkflow.newBuilder().setName("test_workflow_name_basic").build();

    when(migrationServiceInstanceCreator.getMigrationServiceClient())
        .thenReturn(migrationServiceClientMock);
    when(migrationServiceClientMock.createMigrationWorkflow(
            any(CreateMigrationWorkflowRequest.class)))
        .thenReturn(response);

    WorkflowMigrationResponse workflowMigrationResponse =
        workflowMigrationService.createMigrationWorkflow(translateDDLDataDTO);
    Assert.assertEquals(response.getName(), workflowMigrationResponse.getWorkflowName());
  }

  @Test
  public void testMigrationWorkflowLocationEmpty() throws IOException {

    TranslateDDLDataDTO translateDDLDataDTO = new TranslateDDLDataDTO();
    translateDDLDataDTO.setSourceSchemaName("source_public");
    translateDDLDataDTO.setSourceDatabaseName("source_database");
    translateDDLDataDTO.setTranslationJobLocation("us");
    translateDDLDataDTO.setGcsBucketForTranslation("gs://translation");
    translateDDLDataDTO.setSourceDatabaseName("source_database");
    translateDDLDataDTO.setTargetSchemaName("target_schema");
    translateDDLDataDTO.setTargetDatabaseName("target_database");

    when(migrationServiceInstanceCreator.getMigrationServiceClient())
        .thenReturn(MigrationServiceClient.create());
    try {
      workflowMigrationService.createMigrationWorkflow(translateDDLDataDTO);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(
          ErrorCode.MIGRATION_WORKFLOW_EXECUTION_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(
          ErrorCode.MIGRATION_WORKFLOW_EXECUTION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testIsMigrationWorkflowCompleted() {

    MigrationWorkflow response =
        MigrationWorkflow.newBuilder().setState(MigrationWorkflow.State.COMPLETED).build();
    MigrationServiceClient migrationServiceClientMock = mock(MigrationServiceClient.class);

    when(migrationServiceInstanceCreator.getMigrationServiceClient())
        .thenReturn(migrationServiceClientMock);
    when(migrationServiceClientMock.getMigrationWorkflow(anyString())).thenReturn(response);
    boolean returnValue =
        workflowMigrationService.isMigrationWorkflowCompleted("test_workflow_name_completed");
    Assert.assertTrue(returnValue);
  }

  @Test
  public void testIsMigrationWorkflowPaused() {

    MigrationWorkflow response =
        MigrationWorkflow.newBuilder().setState(MigrationWorkflow.State.PAUSED).build();
    MigrationServiceClient migrationServiceClientMock = mock(MigrationServiceClient.class);

    when(migrationServiceInstanceCreator.getMigrationServiceClient())
        .thenReturn(migrationServiceClientMock);
    when(migrationServiceClientMock.getMigrationWorkflow(anyString())).thenReturn(response);
    boolean returnValue =
        workflowMigrationService.isMigrationWorkflowCompleted("test_workflow_name_paused");
    Assert.assertFalse(returnValue);
  }

  @Test
  public void testIsMigrationWorkflowWhileLoopFinishWithNoMatchingStateReturned() {

    MigrationWorkflow response =
        MigrationWorkflow.newBuilder().setState(MigrationWorkflow.State.STATE_UNSPECIFIED).build();
    MigrationServiceClient migrationServiceClientMock = mock(MigrationServiceClient.class);

    when(migrationServiceInstanceCreator.getMigrationServiceClient())
        .thenReturn(migrationServiceClientMock);
    when(migrationServiceClientMock.getMigrationWorkflow(anyString())).thenReturn(response);
    boolean returnValue =
        workflowMigrationService.isMigrationWorkflowCompleted(
            "test_workflow_name_state_unspecified");
    Assert.assertFalse(returnValue);
  }
}
