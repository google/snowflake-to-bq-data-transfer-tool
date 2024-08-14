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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.OperationResult;
import com.google.connector.snowflakeToBQ.model.datadto.SnowflakeUnloadToGCSDataDTO;
import com.google.connector.snowflakeToBQ.service.SnowflakesService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

public class SnowflakeUnloadToGCSAsyncServiceTest extends AbstractTestBase {

  @Autowired SnowflakeUnloadToGCSAsyncService snowflakeUnloadToGCSAsyncService;

  @MockBean SnowflakesService snowflakesService;

  @Test
  public void testSnowflakeUnloadToBQLoad() throws ExecutionException, InterruptedException {
    when(snowflakesService.executeUnloadDataCommand(any(SnowflakeUnloadToGCSDataDTO.class)))
        .thenReturn("1234-abdc-fghi-handle");
    SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO = new SnowflakeUnloadToGCSDataDTO();
    snowflakeUnloadToGCSDataDTO.setTableName("test-table");
    CompletableFuture<OperationResult<String>> returnedResult =
        snowflakeUnloadToGCSAsyncService.snowflakeUnloadToGCS(
            snowflakeUnloadToGCSDataDTO, "test-request-id");
    Assert.assertTrue(returnedResult.get().isSuccess());
    Assert.assertEquals("test-table", returnedResult.get().getResult());
  }

  @Test
  public void testSnowflakeUnloadToBQLoadExceptionCase()
      throws ExecutionException, InterruptedException {
    when(snowflakesService.executeUnloadDataCommand(any(SnowflakeUnloadToGCSDataDTO.class)))
        .thenThrow(SnowflakeConnectorException.class);
    SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO = new SnowflakeUnloadToGCSDataDTO();
    // Intentionally setting null condition
    snowflakeUnloadToGCSDataDTO.setTableName("test-table");
    CompletableFuture<OperationResult<String>> returnedResult =
        snowflakeUnloadToGCSAsyncService.snowflakeUnloadToGCS(
            snowflakeUnloadToGCSDataDTO, "test-request-id");

    Assert.assertFalse(returnedResult.get().isSuccess());
    Assert.assertEquals("Table:test-table,null,0", returnedResult.get().getErrorMessage());
  }
}
