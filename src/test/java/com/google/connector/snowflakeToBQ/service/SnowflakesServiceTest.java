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

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.config.SnowflakeConfigLoader;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.response.SnowflakeResponse;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.mock.mockito.MockBean;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class SnowflakesServiceTest extends AbstractTestBase {
  private static final Logger log = LoggerFactory.getLogger(SnowflakesServiceTest.class);

  private SnowflakesService snowflakesService;
  @MockBean RestAPIExecutionService restAPIExecutionService;

  @MockBean SnowflakeConfigLoader snowflakeConfigLoader;

  String requestBody =
      "{\n"
          + "\"warehouse\":\"Test\",\n"
          + "\"statement\" : \" CREATE or replace STAGE GCS_STAGE_COPY_INTO_%s"
          + " STORAGE_INTEGRATION =  MIGRATION_INTEGRATION1 URL = 'gcs://%s/%s'"
          + " FILE_FORMAT = %s; COPY INTO @GCS_STAGE_COPY_INTO_%s/%s FROM"
          + " %s  OVERWRITE=TRUE HEADER=TRUE \",\n"
          + "\"database\":\"TEST_DATABASE\",\n"
          + "\"schema\":\"PUBLIC\",\n"
          + "\"parameters\": {\n"
          + "      \"MULTI_STATEMENT_COUNT\": \"2\"\n"
          + "  }";

  @Before
  public void setup() {
    snowflakesService =
        new SnowflakesService(
            restAPIExecutionService,
            snowflakeConfigLoader);
  }

  @Test()
  public void testExecuteUnloadDataCommandEmptyResponse() {
    Mono<SnowflakeResponse> s = Mono.just(new SnowflakeResponse());
    when(snowflakeConfigLoader.getSnowflakeUnloadRequestBody(anyString())).thenReturn(requestBody);
    when(restAPIExecutionService.executePostAndPoll(anyString(), anyString()))
        .thenReturn(s);
    when(snowflakeConfigLoader.getQuery("test")).thenReturn("select * from test");
    try {
      snowflakesService.executeUnloadDataCommand("test", "gs:/bucket/test", "SF_GCS_CSV_FORMAT1");
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
      snowflakesService.executeUnloadDataCommand("test", "gs:/bucket/test", "SF_GCS_CSV_FORMAT1");
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
    when(restAPIExecutionService.executePostAndPoll(anyString(), anyString()))
        .thenReturn(s);
    when(snowflakeConfigLoader.getQuery("test")).thenReturn("select * from test");
    try {
      snowflakesService.executeUnloadDataCommand("test1", "gs:/bucket/test", "SF_GCS_CSV_FORMAT1");
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
      snowflakesService.executeUnloadDataCommand("test", "gs:/bucket/test", "SF_GCS_CSV_FORMAT1");
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
    when(restAPIExecutionService.pollWithTimeout(anyString(), anyString()))
        .thenReturn(true);
    String returnValue =
        snowflakesService.executeUnloadDataCommand("test", "gs:/bucket/test", "SF_GCS_CSV_FORMAT1");
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
    when(restAPIExecutionService.pollWithTimeout(anyString(), anyString()))
        .thenReturn(true);
    String returnValue =
        snowflakesService.executeUnloadDataCommand("test", "gs:/bucket/test", "SF_GCS_CSV_FORMAT1");
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
    when(restAPIExecutionService.pollWithTimeout(anyString(), anyString()))
        .thenReturn(false);
    try {
      String returnValue =
          snowflakesService.executeUnloadDataCommand(
              "test", "gs:/bucket/test", "SF_GCS_CSV_FORMAT1");
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.SNOWFLAKE_REST_API_POLL_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.SNOWFLAKE_REST_API_POLL_ERROR.getErrorCode(), e.getErrorCode());
    }
  }
}
