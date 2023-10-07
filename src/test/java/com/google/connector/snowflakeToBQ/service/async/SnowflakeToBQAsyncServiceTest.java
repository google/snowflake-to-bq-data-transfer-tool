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

package com.google.connector.snowflakeToBQ.service.async;

import static com.google.connector.snowflakeToBQ.util.ErrorCode.TABLE_ALREADY_EXISTS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.entity.ApplicationConfigData;
import com.google.connector.snowflakeToBQ.model.OperationResult;
import com.google.connector.snowflakeToBQ.model.datadto.BigQueryDetailsDataDTO;
import com.google.connector.snowflakeToBQ.repository.ApplicationConfigDataRepository;
import com.google.connector.snowflakeToBQ.service.ApplicationConfigDataService;
import com.google.connector.snowflakeToBQ.service.BigQueryOperationsService;
import com.google.connector.snowflakeToBQ.service.GoogleCloudStorageService;
import com.google.connector.snowflakeToBQ.service.SnowflakesService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

public class SnowflakeToBQAsyncServiceTest extends AbstractTestBase {

  @Autowired SnowflakeToBQAsyncService snowflakeToBQAsyncService;

  @Autowired ApplicationConfigDataRepository applicationConfigDataRepository;

  @MockBean BigQueryOperationsService bigQueryOperationsService;
  @MockBean SnowflakesService snowflakesService;
  @MockBean ApplicationConfigDataService applicationConfigDataService;

  @MockBean GoogleCloudStorageService googleCloudStorageService;

  @Before
  public void setUp() {
    applicationConfigDataRepository.deleteAll();
  }

  private static final String CREATE_TABLE =
      "create or replace TABLE `project`.dataset.table2 ( DATECOL DATE,DATETIMECOL DATETIME,TIMESTAMPCOL  TIMESTAMP)";

  @Test
  public void testSnowflakeUnloadToBQLoad() throws ExecutionException, InterruptedException {
    when(bigQueryOperationsService.createTableUsingDDL(any(String.class), any(String.class)))
        .thenReturn(true);
    when(bigQueryOperationsService.loadBigQueryJob(any(BigQueryDetailsDataDTO.class)))
        .thenReturn(true);
    when(snowflakesService.executeUnloadDataCommand(anyString(), anyString(), anyString()))
        .thenReturn("1234-abdc-fghi-handle");
    when(googleCloudStorageService.getContentFromGCSFile(any(String.class), any(String.class)))
        .thenReturn(CREATE_TABLE);
    ApplicationConfigData applicationConfigData = new ApplicationConfigData();
    applicationConfigData.setId(1L);
    applicationConfigData.setBQTableCreated(false);
    applicationConfigData.setTargetDatabaseName("targetdatabase");
    applicationConfigData.setTargetSchemaName("targetschema");
    applicationConfigData.setTargetTableName("targettablename");

    CompletableFuture<OperationResult<ApplicationConfigData>> returnedResult =
        snowflakeToBQAsyncService.snowflakeUnloadAndLoadToBQLoad(applicationConfigData);
    Assert.assertTrue(returnedResult.get().isSuccess());
    Assert.assertTrue(returnedResult.get().getResult().isBQTableCreated());
    Assert.assertTrue(returnedResult.get().getResult().isDataUnloadedFromSnowflake());
    Assert.assertTrue(returnedResult.get().getResult().isDataLoadedInBQ());
  }

  @Test
  public void testSnowflakeUnloadToBQLoadTableExists()
      throws ExecutionException, InterruptedException {
    // Return false to reproduce the scenario of table already exists
    when(bigQueryOperationsService.createTableUsingDDL(any(String.class), any(String.class)))
        .thenReturn(false);
    when(googleCloudStorageService.getContentFromGCSFile(any(String.class), any(String.class)))
        .thenReturn(CREATE_TABLE);
    when(bigQueryOperationsService.isTableExists(any(BigQueryDetailsDataDTO.class)))
        .thenReturn(true);
    ApplicationConfigData applicationConfigData = new ApplicationConfigData();
    applicationConfigData.setId(1L);
    applicationConfigData.setBQTableCreated(false);
    applicationConfigData.setTargetDatabaseName("targetdatabase");
    applicationConfigData.setTargetSchemaName("targetschema");
    applicationConfigData.setTargetTableName("targettablename");

    CompletableFuture<OperationResult<ApplicationConfigData>> returnedResult =
        snowflakeToBQAsyncService.snowflakeUnloadAndLoadToBQLoad(applicationConfigData);
    Assert.assertFalse(returnedResult.get().isSuccess());
    Assert.assertEquals(
        String.format(
            "%s, %s, Error Code:%s",
            applicationConfigData.getTargetTableName(),
            TABLE_ALREADY_EXISTS.getMessage(),
            TABLE_ALREADY_EXISTS.getErrorCode()),
        returnedResult.get().getErrorMessage());
    Assert.assertNull(returnedResult.get().getResult());
  }

  @Test
  public void testSnowflakeUnloadToBQLoadManyConditionAlreadyDone()
      throws ExecutionException, InterruptedException {
    when(bigQueryOperationsService.loadBigQueryJob(any(BigQueryDetailsDataDTO.class)))
        .thenReturn(true);
    ApplicationConfigData applicationConfigData = new ApplicationConfigData();
    applicationConfigData.setId(1L);
    // Setting this true to reproduce a situation where previous job was  done till the point of
    // table creation, and now that steps is not needed(skipped) when we are rerunning that job
    // rather other
    // steps needs to be executed
    applicationConfigData.setBQTableCreated(true);
    // Setting this true to reproduce a situation where previous job was done till the point of
    // data unloading from Snowflake, and now that steps is not needed when we are rerunning that
    // job rather other
    // steps needs to be executed
    applicationConfigData.setDataUnloadedFromSnowflake(true);
    // Setting this true to reproduce a situation where previous job was done till the point of
    // data loaded in BQ, and now that steps is not needed when we are rerunning that job rather
    // other
    // steps needs to be executed
    applicationConfigData.setDataLoadedInBQ(true);
    applicationConfigData.setTargetDatabaseName("targetdatabase");
    applicationConfigData.setTargetSchemaName("targetschema");
    applicationConfigData.setTargetTableName("targettablename");

    CompletableFuture<OperationResult<ApplicationConfigData>> retrunedResult =
        snowflakeToBQAsyncService.snowflakeUnloadAndLoadToBQLoad(applicationConfigData);
    Assert.assertTrue(retrunedResult.get().isSuccess());
    Assert.assertTrue(retrunedResult.get().getResult().isBQTableCreated());
    Assert.assertTrue(retrunedResult.get().getResult().isDataUnloadedFromSnowflake());
    Assert.assertTrue(retrunedResult.get().getResult().isDataLoadedInBQ());
  }

  @Test
  public void testSnowflakeUnloadToBQLoadExceptionInBQLoadJOb()
      throws ExecutionException, InterruptedException {
    doThrow(RuntimeException.class)
        .when(bigQueryOperationsService)
        .loadBigQueryJob(any(BigQueryDetailsDataDTO.class));
    ApplicationConfigData applicationConfigData = new ApplicationConfigData();
    applicationConfigData.setId(1L);
    // Setting this true to reproduce a situation where previous job was  done till the point of
    // table creation, and now that steps is not needed(skipped) when we are rerunning that job
    // rather other
    // steps needs to be executed
    applicationConfigData.setBQTableCreated(true);
    // Setting this true to reproduce a situation where previous job was done till the point of
    // data unloading from Snowflake, and now that steps is not needed when we are rerunning that
    // job rather other
    // steps needs to be executed
    applicationConfigData.setDataUnloadedFromSnowflake(true);
    applicationConfigData.setDataLoadedInBQ(false);
    applicationConfigData.setTargetDatabaseName("targetdatabase");
    applicationConfigData.setTargetSchemaName("targetschema");
    applicationConfigData.setTargetTableName("targettablename");

    CompletableFuture<OperationResult<ApplicationConfigData>> retrunedResult =
        snowflakeToBQAsyncService.snowflakeUnloadAndLoadToBQLoad(applicationConfigData);
    Assert.assertFalse(retrunedResult.get().isSuccess());
    Assert.assertNull(retrunedResult.get().getResult());
    Assert.assertEquals("targettablename, Error:null", retrunedResult.get().getErrorMessage());
  }
}
