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
import static org.mockito.Mockito.when;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.config.SnowflakeConfigLoader;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.SnowflakeUnloadToGCSDataDTO;
import com.google.connector.snowflakeToBQ.model.response.SnowflakeResponse;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;
import reactor.core.publisher.Mono;

public class SnowflakesServiceTest extends AbstractTestBase {
  private static final Logger log = LoggerFactory.getLogger(SnowflakesServiceTest.class);

  private SnowflakesService snowflakesService;

  @MockBean RestAPIExecutionService restAPIExecutionService;

  @MockBean SnowflakeConfigLoader snowflakeConfigLoader;

  @Value("${gcs.storage.integration}")
  String gcsStorageIntegration;

  String requestBody =
      "{\n"
          + "  \"UnloadDataRequest\": {\n"
          + "    \"warehouse\": \"{{WAREHOUSE}}\",\n"
          + "    \"statement\": \"BEGIN; ALTER SESSION SET QUERY_TAG = 'BQ-MIGRATION-{{TABLE_NAME}}'; USE DATABASE {{DATABASE}}; USE SCHEMA {{SCHEMA}}; CREATE OR REPLACE STAGE GCS_STAGE_COPY_INTO_{{TABLE_NAME}} STORAGE_INTEGRATION = {{STORAGE_INTEGRATION}} URL = 'gcs://{{STAGE_LOCATION}}/{{TABLE_NAME}}' FILE_FORMAT = {{FILE_FORMAT}}; COPY INTO @GCS_STAGE_COPY_INTO_{{TABLE_NAME}}/{{TABLE_NAME}} FROM {{SNOWFLAKE_QUERY}}  OVERWRITE=TRUE HEADER=TRUE; COMMIT;\",\n"
          + "    \"parameters\": {\n"
          + "      \"MULTI_STATEMENT_COUNT\": \"7\"\n"
          + "    }\n"
          + "  }\n"
          + "}\n";

  @Before
  public void setup() {
    snowflakesService = new SnowflakesService(restAPIExecutionService, snowflakeConfigLoader);
    snowflakesService.setGcsStorageIntegration(gcsStorageIntegration);
  }

  @Test()
  public void testExecuteUnloadDataCommandEmptyResponse() {
    Mono<SnowflakeResponse> s = Mono.just(new SnowflakeResponse());
    when(snowflakeConfigLoader.getSnowflakeUnloadRequestBody(anyString())).thenReturn(requestBody);
    when(restAPIExecutionService.executePostAndPoll(anyString(), anyString())).thenReturn(s);
    when(snowflakeConfigLoader.getQuery("test")).thenReturn("select * from test");
    try {
      SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO = new SnowflakeUnloadToGCSDataDTO();
      snowflakeUnloadToGCSDataDTO.setDatabaseName("TEST_DATABASE");
      snowflakeUnloadToGCSDataDTO.setSchemaName("public");
      snowflakeUnloadToGCSDataDTO.setTableName("test");
      snowflakeUnloadToGCSDataDTO.setSnowflakeStageLocation("gs:/bucket/test");
      snowflakeUnloadToGCSDataDTO.setSnowflakeFileFormatValue("SF_GCS_CSV_FORMAT1");
      snowflakeUnloadToGCSDataDTO.setWarehouse("MIGRATION_WAREHOUSE");
      snowflakesService.executeUnloadDataCommand(snowflakeUnloadToGCSDataDTO);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.SNOWFLAKE_UNLOAD_DATA_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.SNOWFLAKE_UNLOAD_DATA_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test()
  public void testExecuteUnloadDataCommandEmptyResponseCondition1() {
    SnowflakeResponse sf = new SnowflakeResponse();
    sf.setMessage("");
    sf.setRequestId("48e23917-besdcsdb-4054-afbb-14d68e3b20f8");
    sf.setSqlState("0000");
    log.info(
        sf.getMessage() + " " + sf.getRequestId() + " " + sf.getSqlState() + " " + sf.toString());
    Mono<SnowflakeResponse> responseMono = Mono.just(sf);

    when(snowflakeConfigLoader.getSnowflakeUnloadRequestBody(anyString())).thenReturn(requestBody);
    when(restAPIExecutionService.executePostAndPoll(anyString(), anyString()))
        .thenReturn(responseMono);
    when(snowflakeConfigLoader.getQuery("test")).thenReturn("select * from test");
    try {
      SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO = new SnowflakeUnloadToGCSDataDTO();
      snowflakeUnloadToGCSDataDTO.setDatabaseName("TEST_DATABASE");
      snowflakeUnloadToGCSDataDTO.setSchemaName("public");
      snowflakeUnloadToGCSDataDTO.setTableName("test");
      snowflakeUnloadToGCSDataDTO.setSnowflakeStageLocation("gs:/bucket/test");
      snowflakeUnloadToGCSDataDTO.setSnowflakeFileFormatValue("SF_GCS_CSV_FORMAT1");
      snowflakeUnloadToGCSDataDTO.setWarehouse("MIGRATION_WAREHOUSE");
      snowflakesService.executeUnloadDataCommand(snowflakeUnloadToGCSDataDTO);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.SNOWFLAKE_UNLOAD_DATA_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.SNOWFLAKE_UNLOAD_DATA_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test()
  public void testExecuteUnloadDataCommandEmptyResponseEmptyTableMap() {
    Mono<SnowflakeResponse> s = Mono.just(new SnowflakeResponse());

    when(snowflakeConfigLoader.getSnowflakeUnloadRequestBody(anyString())).thenReturn(requestBody);
    when(restAPIExecutionService.executePostAndPoll(anyString(), anyString())).thenReturn(s);
    when(snowflakeConfigLoader.getQuery("test")).thenReturn("select * from test");
    try {
      SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO = new SnowflakeUnloadToGCSDataDTO();
      snowflakeUnloadToGCSDataDTO.setDatabaseName("TEST_DATABASE");
      snowflakeUnloadToGCSDataDTO.setSchemaName("public");
      snowflakeUnloadToGCSDataDTO.setTableName("test");
      snowflakeUnloadToGCSDataDTO.setSnowflakeStageLocation("gs:/bucket/test");
      snowflakeUnloadToGCSDataDTO.setSnowflakeFileFormatValue("SF_GCS_CSV_FORMAT1");
      snowflakeUnloadToGCSDataDTO.setWarehouse("MIGRATION_WAREHOUSE");
      snowflakesService.executeUnloadDataCommand(snowflakeUnloadToGCSDataDTO);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.SNOWFLAKE_UNLOAD_DATA_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.SNOWFLAKE_UNLOAD_DATA_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test()
  public void testExecuteUnloadDataCommandNullResponse() {

    Mono<SnowflakeResponse> responseMono = Mono.just(new SnowflakeResponse());

    when(snowflakeConfigLoader.getSnowflakeUnloadRequestBody(anyString())).thenReturn(requestBody);
    when(restAPIExecutionService.executePostAndPoll(anyString(), anyString()))
        .thenReturn(responseMono)
        .thenReturn(Mono.empty());
    when(snowflakeConfigLoader.getQuery("test")).thenReturn("select * from test");
    try {
      SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO = new SnowflakeUnloadToGCSDataDTO();
      snowflakeUnloadToGCSDataDTO.setDatabaseName("TEST_DATABASE");
      snowflakeUnloadToGCSDataDTO.setSchemaName("public");
      snowflakeUnloadToGCSDataDTO.setTableName("test");
      snowflakeUnloadToGCSDataDTO.setSnowflakeStageLocation("gs:/bucket/test");
      snowflakeUnloadToGCSDataDTO.setSnowflakeFileFormatValue("SF_GCS_CSV_FORMAT1");
      snowflakeUnloadToGCSDataDTO.setWarehouse("MIGRATION_WAREHOUSE");
      snowflakesService.executeUnloadDataCommand(snowflakeUnloadToGCSDataDTO);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.SNOWFLAKE_UNLOAD_DATA_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.SNOWFLAKE_UNLOAD_DATA_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test()
  public void testExecuteUnloadDataCommandWithoutPolling() {
    SnowflakeResponse sf = new SnowflakeResponse();
    String statementHandle = UUID.randomUUID().toString();
    sf.setStatementHandle(statementHandle);
    sf.setMessage("Statement executed successfully.");
    Mono<SnowflakeResponse> responseMono = Mono.just(sf);

    when(snowflakeConfigLoader.getSnowflakeUnloadRequestBody(anyString())).thenReturn(requestBody);
    when(restAPIExecutionService.executePostAndPoll(anyString(), anyString()))
        .thenReturn(responseMono);
    when(restAPIExecutionService.pollWithTimeout(anyString(), anyString())).thenReturn(true);
    SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO = new SnowflakeUnloadToGCSDataDTO();
    snowflakeUnloadToGCSDataDTO.setDatabaseName("TEST_DATABASE");
    snowflakeUnloadToGCSDataDTO.setSchemaName("public");
    snowflakeUnloadToGCSDataDTO.setTableName("test");
    snowflakeUnloadToGCSDataDTO.setSnowflakeStageLocation("gs:/bucket/test");
    snowflakeUnloadToGCSDataDTO.setSnowflakeFileFormatValue("SF_GCS_CSV_FORMAT1");
    snowflakeUnloadToGCSDataDTO.setWarehouse("MIGRATION_WAREHOUSE");
    String returnValue = snowflakesService.executeUnloadDataCommand(snowflakeUnloadToGCSDataDTO);

    Assert.assertEquals(statementHandle, returnValue);
  }

  @Test()
  public void testExecuteUnloadDataCommandNeedPolling() {
    SnowflakeResponse sf = new SnowflakeResponse();
    String statementHandle = UUID.randomUUID().toString();
    sf.setStatementHandle(statementHandle);
    sf.setMessage("Statement executed successfully");
    Mono<SnowflakeResponse> responseMono = Mono.just(sf);

    when(snowflakeConfigLoader.getSnowflakeUnloadRequestBody(anyString())).thenReturn(requestBody);
    when(restAPIExecutionService.executePostAndPoll(anyString(), anyString()))
        .thenReturn(responseMono);
    when(restAPIExecutionService.pollWithTimeout(anyString(), anyString())).thenReturn(true);
    SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO = new SnowflakeUnloadToGCSDataDTO();
    snowflakeUnloadToGCSDataDTO.setDatabaseName("TEST_DATABASE");
    snowflakeUnloadToGCSDataDTO.setSchemaName("public");
    snowflakeUnloadToGCSDataDTO.setTableName("test");
    snowflakeUnloadToGCSDataDTO.setSnowflakeStageLocation("gs:/bucket/test");
    snowflakeUnloadToGCSDataDTO.setSnowflakeFileFormatValue("SF_GCS_CSV_FORMAT1");
    snowflakeUnloadToGCSDataDTO.setWarehouse("MIGRATION_WAREHOUSE");
    String returnValue = snowflakesService.executeUnloadDataCommand(snowflakeUnloadToGCSDataDTO);
    Assert.assertEquals(statementHandle, returnValue);
  }

  @Test()
  public void testExecuteUnloadDataCommandNeedPollingError() {
    SnowflakeResponse sf = new SnowflakeResponse();
    String statementHandle = UUID.randomUUID().toString();
    sf.setStatementHandle(statementHandle);
    sf.setMessage("Statement executed successfully");
    Mono<SnowflakeResponse> responseMono = Mono.just(sf);

    when(snowflakeConfigLoader.getSnowflakeUnloadRequestBody(anyString())).thenReturn(requestBody);
    when(restAPIExecutionService.executePostAndPoll(anyString(), anyString()))
        .thenReturn(responseMono);
    when(restAPIExecutionService.pollWithTimeout(anyString(), anyString())).thenReturn(false);
    try {
      SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO = new SnowflakeUnloadToGCSDataDTO();
      snowflakeUnloadToGCSDataDTO.setDatabaseName("TEST_DATABASE");
      snowflakeUnloadToGCSDataDTO.setSchemaName("public");
      snowflakeUnloadToGCSDataDTO.setTableName("test");
      snowflakeUnloadToGCSDataDTO.setSnowflakeStageLocation("gs:/bucket/test");
      snowflakeUnloadToGCSDataDTO.setSnowflakeFileFormatValue("SF_GCS_CSV_FORMAT1");
      snowflakeUnloadToGCSDataDTO.setWarehouse("MIGRATION_WAREHOUSE");
      String returnValue = snowflakesService.executeUnloadDataCommand(snowflakeUnloadToGCSDataDTO);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.SNOWFLAKE_REST_API_POLL_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.SNOWFLAKE_REST_API_POLL_ERROR.getErrorCode(), e.getErrorCode());
    }
  }
}
