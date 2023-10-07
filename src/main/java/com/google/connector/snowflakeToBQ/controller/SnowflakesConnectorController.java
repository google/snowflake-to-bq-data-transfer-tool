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

package com.google.connector.snowflakeToBQ.controller;

import static com.google.connector.snowflakeToBQ.util.PropertyManager.OUTPUT_FORMATTER1;

import com.google.connector.snowflakeToBQ.config.OAuthCredentials;
import com.google.connector.snowflakeToBQ.model.OperationResult;
import com.google.connector.snowflakeToBQ.model.request.SFDataMigrationRequestDTO;
import com.google.connector.snowflakeToBQ.model.request.SFExtractAndTranslateDDLRequestDTO;
import com.google.connector.snowflakeToBQ.model.request.SnowflakeUnloadToGCSRequestDTO;
import com.google.connector.snowflakeToBQ.service.ApplicationConfigDataService;
import com.google.connector.snowflakeToBQ.service.ExtractAndTranslateDDLService;
import com.google.connector.snowflakeToBQ.service.SnowflakeMigrateDataService;
import com.google.connector.snowflakeToBQ.service.TokenRefreshService;
import com.google.connector.snowflakeToBQ.service.async.SnowflakeUnloadToGCSAsyncService;
import com.google.connector.snowflakeToBQ.util.PropertyManager;
import com.google.connector.snowflakeToBQ.util.encryption.EncryptValues;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.*;

/** Class to handle HTTPS/HTTPS request and return the response back to caller. */
@RestController
@RequestMapping("/connector")
public class SnowflakesConnectorController {
  private static final Logger log = LoggerFactory.getLogger(SnowflakesConnectorController.class);
  public static final String REQUEST_LOG_ID = "requestLogId";
  final EncryptValues encryptValues;
  final TokenRefreshService tokenRefreshService;
  final SnowflakeMigrateDataService snowflakeMigrateDataService;
  final ExtractAndTranslateDDLService extractDDLService;
  final SnowflakeUnloadToGCSAsyncService snowflakeUnloadToGCSAsyncService;

  final ApplicationConfigDataService applicationConfigDataService;

  final OAuthCredentials oauthCredentials;

  public SnowflakesConnectorController(
      EncryptValues encryptValues,
      TokenRefreshService tokenRefreshService,
      SnowflakeMigrateDataService snowflakeMigrateDataService,
      OAuthCredentials oauthCredentials,
      ExtractAndTranslateDDLService extractDDLService,
      SnowflakeUnloadToGCSAsyncService snowflakeUnloadToGCSAsyncService,
      ApplicationConfigDataService applicationConfigDataService) {
    this.encryptValues = encryptValues;
    this.tokenRefreshService = tokenRefreshService;
    this.snowflakeMigrateDataService = snowflakeMigrateDataService;
    this.oauthCredentials = oauthCredentials;
    this.extractDDLService = extractDDLService;
    this.snowflakeUnloadToGCSAsyncService = snowflakeUnloadToGCSAsyncService;
    this.applicationConfigDataService = applicationConfigDataService;
  }

  /**
   * Method/API to receive the Snowflake migration request to migrate a table/schema/database from
   * snowflakes to BigQuery. Its a full migration request which performs, extract ddl, translate
   * ddl, Snowflake data unload to GCS and the load it to BigQuery after creating the table(if
   * request). This API should be used if user need all four steps otherwise other request API can
   * be referred which performs individual operations
   *
   * @param sfDataMigrationRequestDTO Its contains data sent by a client as a part of this
   *     HTTP/HTTPS request. It contains all the required parameter for performing the migration.
   * @return response of the migration in @{@link String} Response just gives a message indicating
   *     the flow is complete
   */
  @PostMapping("/migrate-data")
  public ResponseEntity<?> migrateData(
      @NonNull @RequestBody @Valid SFDataMigrationRequestDTO sfDataMigrationRequestDTO) {
    /*
     * MDC (Mapped Diagnostic Context): MDC is a feature provided by logging frameworks to allow the
     * association of key-value pairs with a thread of execution. These key-value pairs can be used
     * to enrich log entries with context-specific information. For example, we have associate a
     * unique "requestId" with each incoming HTTP request to track log entries related to that
     * specific request.
     */
    MDC.put(REQUEST_LOG_ID, UUID.randomUUID().toString());
    List<Long> requestIds = snowflakeMigrateDataService.migrateData(sfDataMigrationRequestDTO);
    // Removing the MDC key which is associated in the MDC

    if (requestIds != null) {
      return ResponseEntity.ok(
          snowflakeMigrateDataService.getApplicationConfigDataByIds(requestIds));
    } else {
      return ResponseEntity.internalServerError()
          .body(
              String.format(
                  "Internal server error while processing the request, please check the logs. Request Log Id:%s",
                  MDC.get(REQUEST_LOG_ID)));
    }
  }

  /**
   * Method to receive requests to process failed requests. This application stores the request data
   * in an embedded database. The request data contains all the data related to the table for
   * migration. This application stores data for different stages like DDL extraction, translation,
   * Snowflake unload, etc. This step will process the failed requests which have passed the
   * Snowflake unload step. This means that if the extraction DDL, translation DDL and Snowflake
   * unload are successfully executed for any request, but it failed while executing the next steps,
   * then this method can be used to reprocess those requests.
   *
   * @return response of the migration in @{@link String} Response just gives a message indicating
   *     the flow is complete
   */
  @GetMapping("/process-failed-request")
  public ResponseEntity<?> processFailedRequest() {
    MDC.put(REQUEST_LOG_ID, UUID.randomUUID().toString());
    List<Long> requestIds = snowflakeMigrateDataService.processFailedRequestMigratedRows();
    MDC.remove(REQUEST_LOG_ID);
    if (requestIds != null) {
      return ResponseEntity.ok(
          snowflakeMigrateDataService.getApplicationConfigDataByIds(requestIds));
    } else {
      return ResponseEntity.internalServerError()
          .body(
              String.format(
                  "Internal server error while processing the request, please check the logs. Request Log Id:%s",
                  MDC.get(REQUEST_LOG_ID)));
    }
  }

  /**
   * Method/API to receive the request just for extract the DDL from Snowflake and translate it to
   * BigQuery DDL format.
   *
   * @param extractAndTranslateDDLRequestDTO Its contains data sent by a client as a part of this *
   *     HTTP/HTTPS request. It contains all the required parameter for performing the extract and
   *     translate DDL operation.
   * @return response of the operation in @{@link String}. Response just gives a message indicating
   *     the flow is complete
   */
  @PostMapping("/extract-ddl")
  public ResponseEntity<String> extractDDL(
      @NonNull @RequestBody @Valid
          SFExtractAndTranslateDDLRequestDTO extractAndTranslateDDLRequestDTO) {
    MDC.put(REQUEST_LOG_ID, UUID.randomUUID().toString());
    String response = extractDDLService.extractAndTranslateDDLs(extractAndTranslateDDLRequestDTO);
    response = String.format("%s. Request Log Id:%s", response, MDC.get(REQUEST_LOG_ID));
    MDC.remove(REQUEST_LOG_ID);
    return ResponseEntity.ok(response);
  }

  /**
   * Method/API to receive a request for Unload the table data from Snowflake to GCS.
   *
   * @param snowflakeUnloadToGCSRequestDTO Its contains data sent by a client as a part of this * *
   *     HTTP/HTTPS request. It contains all the required parameter for performing the Snowflake
   *     unload operation.
   * @return response of the operation in @{@link String}. Response just gives a message indicating
   *     * the flow is complete
   */
  @PostMapping("/snowflake-unload-to-gcs")
  public ResponseEntity<Map<String, String>> snowFlakeUnloadToGCS(
      @NonNull @RequestBody @Valid SnowflakeUnloadToGCSRequestDTO snowflakeUnloadToGCSRequestDTO) {
    String requestLogId = UUID.randomUUID().toString();
    MDC.put(REQUEST_LOG_ID, requestLogId);

    List<CompletableFuture<OperationResult<String>>> asyncFutureResultList = new ArrayList<>();

    // Executing all the request for each table in parallel(Async)
    for (String tableName : snowflakeUnloadToGCSRequestDTO.getTableNames()) {
      CompletableFuture<OperationResult<String>> asyncFutureResult =
          snowflakeUnloadToGCSAsyncService.snowflakeUnloadToGCS(
              tableName,
              snowflakeUnloadToGCSRequestDTO.getSnowflakeStageLocation(),
              snowflakeUnloadToGCSRequestDTO.getSnowflakeFileFormatValue(),
              requestLogId);
      asyncFutureResultList.add(asyncFutureResult);
    }
    Map<String, String> outputMap = new HashMap<>();
    for (CompletableFuture<OperationResult<String>> tempAsyncFutureResult : asyncFutureResultList) {
      try {

        OperationResult<String> tempOperationResult = tempAsyncFutureResult.join();

        if (tempOperationResult.isSuccess()) {
          // Setting the processing of the row to done
          outputMap.put(tempOperationResult.getResult(), "Success");
        } else {
          log.error("Error:Snowflake Unload and GCS");
          outputMap.put(tempOperationResult.getErrorMessage(), "Failed");
        }
      } catch (CompletionException e) {
        log.error(
            "Exception while processing the Snowflake unload to GCS, error message:{}",
            e.getMessage());
        outputMap.put(e.getMessage(), "Failed");
      }
    }
    outputMap.put("requestLogId", MDC.get(REQUEST_LOG_ID));
    MDC.remove(REQUEST_LOG_ID);
    return ResponseEntity.ok(outputMap);
  }

  /**
   * This method saves the received values in the OAuthCredential Map. There is no restriction of
   * what all credentialsData can be sent but three important fields are required (clientId,
   * clientSecret, refreshToken). Method encrypts the received data and then save it in the map
   * which gets used accross application whenever needed.
   *
   * @param credentialsData map containing the credentials data
   * @return response of the operation in @{@link String}. Response just gives a message indicating
   *     the flow is complete
   */
  @PostMapping("/save-oauth-values")
  public String encryptAndSaveOAuthValue(
      @RequestBody @NotNull @Size(min = 1) Map<String, String> credentialsData) {
    MDC.put(REQUEST_LOG_ID, UUID.randomUUID().toString());

    Map<String, String> encryptedMap = new HashMap<>();
    for (Map.Entry<String, String> entry : credentialsData.entrySet()) {
      encryptedMap.put(entry.getKey(), encryptValues.encryptValue(entry.getValue()));
    }
    oauthCredentials.populateOauthMap(encryptedMap);
    // We need to refresh the access token after setting the new clientId, secret and refresh token
    // value received as a part of this request. This will be more useful in a scenario
    // where already set refresh token is expired and the scheduler is not run after this request
    // execution. Using the below line, when user set the new value it will be refreshed, so the map
    // will  contain  the new access token value.
    tokenRefreshService.refreshToken();
    String message =
        String.format(
            "OAUTH credentials have been successfully saved at %s,Request Log Id:%s",
            PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1),
            MDC.get(REQUEST_LOG_ID));
    log.info(message);
    MDC.remove(REQUEST_LOG_ID);
    return message;
  }

  /**
   * This method can be used to refresh the Oauth token and fetch the new access token from
   * Snowflake.
   */
  @GetMapping("/refresh-token")
  public String refreshOauthToken() {
    MDC.put(REQUEST_LOG_ID, UUID.randomUUID().toString());

    tokenRefreshService.refreshToken();
    String message =
        String.format(
            "Token refresh completed at %s,Request Log Id:%s",
            PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1),
            MDC.get(REQUEST_LOG_ID));
    log.info(message);
    MDC.remove(REQUEST_LOG_ID);
    return message;
  }

  /**
   * Method to encrypt any received valus in map. It then returns the encrypted value to the called.
   *
   * @param data Map containing the values to be encrypted
   * @return encrypted value map.
   */
  @PostMapping("/encrypt-values")
  public Map<String, String> encryptValue(
      @RequestBody @NotNull @Size(min = 1) Map<String, String> data) {
    MDC.put(REQUEST_LOG_ID, UUID.randomUUID().toString());

    Map<String, String> encryptedMap = new HashMap<>();
    for (Map.Entry<String, String> entry : data.entrySet()) {
      log.info("Key:{}, Value:{}", entry.getKey(), entry.getValue());
      encryptedMap.put(entry.getKey(), encryptValues.encryptValue(entry.getValue()));
    }
    log.info(
        "Received valus encryption completed at {}",
        PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
    MDC.remove(REQUEST_LOG_ID);
    return encryptedMap;
  }
}
