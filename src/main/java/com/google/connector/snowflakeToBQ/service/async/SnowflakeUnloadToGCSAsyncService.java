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

import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.OperationResult;
import com.google.connector.snowflakeToBQ.service.SnowflakesService;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * Class to perform Snowflake data unload to GCS in Asynchronous way. All requests carrying out the
 * same tasks will run in parallel with each other, depending on the Thread Executor settings. This
 * is using the spring boot Async feature.
 */
@Service
public class SnowflakeUnloadToGCSAsyncService {
  private static final Logger log = LoggerFactory.getLogger(SnowflakeUnloadToGCSAsyncService.class);

  final SnowflakesService snowflakesService;

  public SnowflakeUnloadToGCSAsyncService(SnowflakesService snowflakesService) {
    this.snowflakesService = snowflakesService;
  }

  /**
   * Method to perform the Snowflake table data unloading to GCS in Asynchronous way. Here
   * "customExecutor" annotation is used which defined the Thread executor and its property.
   *
   * @param tableName name of the table to be unloaded from Snowflake
   * @param snowflakeStageLocation staging location given in the copy into command. This will be the
   *     GCS path which gets configured beforehand
   * @param snowflakeFileFormatValue Snowflake format in which data will be unloaded like Parquet,
   *     CSV
   * @return @{@link CompletableFuture} result of the execution, success or fail
   */
  @Async("customExecutor")
  public CompletableFuture<OperationResult<String>> snowflakeUnloadToGCS(
      String tableName,
      String snowflakeStageLocation,
      String snowflakeFileFormatValue,
      String requestLogId) {

    // We are using Aysnc method here so the method which is calling it can not pass its context to
    // this method directly and requestId was coming as null which was set in controller. Hence we
    // have saved it in the application data and used it to set again in this method in MDC. This
    // way any following method call from this method will log that entry, and we will get same
    // requestId logged from Controller till the end of all the calls.

    MDC.put("requestLogId", requestLogId);

    log.info("Inside SnowflakeUnloadToBQLoad() of SnowflakeUnloadToGCSAsyncService");
    try {
      String snowflakeStatementHandle =
          snowflakesService.executeUnloadDataCommand(
              tableName, snowflakeStageLocation, snowflakeFileFormatValue);
      log.info(
          "Snowflake statement handle:: {}, for table name:: {}",
          snowflakeStatementHandle,
          tableName);
    } catch (SnowflakeConnectorException e) {
      return CompletableFuture.completedFuture(
          new OperationResult<>(
              new OperationResult.Error(
                  String.format("Table:%s,%s,%s", tableName, e.getMessage(), e.getErrorCode()))));
    }
    MDC.remove("requestLogId");
    return CompletableFuture.completedFuture(new OperationResult<>(tableName));
  }
}
