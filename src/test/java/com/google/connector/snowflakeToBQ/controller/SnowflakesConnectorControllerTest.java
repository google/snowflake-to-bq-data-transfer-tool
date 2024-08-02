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

package com.google.connector.snowflakeToBQ.controller;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.config.OAuthCredentials;
import com.google.connector.snowflakeToBQ.model.EncryptedData;
import com.google.connector.snowflakeToBQ.model.OperationResult;
import com.google.connector.snowflakeToBQ.model.datadto.SnowflakeUnloadToGCSDataDTO;
import com.google.connector.snowflakeToBQ.model.request.SFDataMigrationRequestDTO;
import com.google.connector.snowflakeToBQ.model.request.SFExtractAndTranslateDDLRequestDTO;
import com.google.connector.snowflakeToBQ.model.request.SnowflakeUnloadToGCSRequestDTO;
import com.google.connector.snowflakeToBQ.model.response.SFDataMigrationResponse;
import com.google.connector.snowflakeToBQ.model.response.TokenResponse;
import com.google.connector.snowflakeToBQ.service.ExtractAndTranslateDDLService;
import com.google.connector.snowflakeToBQ.service.SnowflakeMigrateDataService;
import com.google.connector.snowflakeToBQ.service.TokenRefreshService;
import com.google.connector.snowflakeToBQ.service.async.SnowflakeUnloadToGCSAsyncService;
import com.google.connector.snowflakeToBQ.util.CommonMethods;
import com.google.connector.snowflakeToBQ.util.encryption.EncryptValues;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.ResponseEntity;

public class SnowflakesConnectorControllerTest extends AbstractTestBase {

  @Autowired SnowflakesConnectorController snowflakesConnectorController;
  @Autowired OAuthCredentials oAuthCredentials;

  @MockBean EncryptValues encryptValues;
  @MockBean TokenRefreshService tokenRefreshService;
  @MockBean SnowflakeMigrateDataService snowflakeMigrateDataService;
  @MockBean SnowflakeUnloadToGCSAsyncService snowflakeUnloadToGCSAsyncService;
  @MockBean ExtractAndTranslateDDLService extractAndTranslateDDLService;

  @Test
  public void testMigrateData() {
    SFDataMigrationResponse sfDataMigrationResponse = new SFDataMigrationResponse();
    List<SFDataMigrationResponse> sfDataMigrationResponses = new ArrayList<>();
    List<Long> requestIds = new ArrayList<>();
    requestIds.add(1L);
    sfDataMigrationResponse.setSuccess(true);
    sfDataMigrationResponse.setSourceTableName("source_table");
    sfDataMigrationResponses.add(sfDataMigrationResponse);
    when(snowflakeMigrateDataService.migrateData(any(SFDataMigrationRequestDTO.class)))
        .thenReturn(requestIds);
    when(snowflakeMigrateDataService.getApplicationConfigDataByIds(any(List.class)))
        .thenReturn(sfDataMigrationResponses);
    Object outputBody =
        snowflakesConnectorController.migrateData(new SFDataMigrationRequestDTO()).getBody();
    List<?> responseList = (List<?>) outputBody;
    List<SFDataMigrationResponse> outCastedList = (List<SFDataMigrationResponse>) responseList;
    Assert.assertTrue(outCastedList.get(0).isSuccess());
  }

  @Test
  public void testMigrateDataElseCondition() {
    List<Long> requestIds = null;
    when(snowflakeMigrateDataService.migrateData(any(SFDataMigrationRequestDTO.class)))
        .thenReturn(requestIds);
    Object outputBody =
        snowflakesConnectorController.migrateData(new SFDataMigrationRequestDTO()).getBody();
    Assert.assertTrue(
        outputBody
            .toString()
            .contains(
                "Internal server error while processing the request, please check the logs."));
  }

  @Test
  public void testSnowflakeUnloadToGCS() {
    SnowflakeUnloadToGCSRequestDTO snowflakeUnloadToGCSRequestDTO =
        new SnowflakeUnloadToGCSRequestDTO();
    snowflakeUnloadToGCSRequestDTO.setSnowflakeStageLocation("");
    snowflakeUnloadToGCSRequestDTO.setSourceTableName("table1");
    snowflakeUnloadToGCSRequestDTO.setSnowflakeStageLocation(
        "snowflake-to-gcs-migration/data-unload");
    snowflakeUnloadToGCSRequestDTO.setSnowflakeFileFormatValue("SF_GCS_CSV_FORMAT1");
    CompletableFuture<OperationResult<String>> returnedResult =
        CompletableFuture.completedFuture(new OperationResult<>("Table1"));
    when(snowflakeUnloadToGCSAsyncService.snowflakeUnloadToGCS(
            (any(SnowflakeUnloadToGCSDataDTO.class)), anyString()))
        .thenReturn(returnedResult);
    ResponseEntity<?> responseEntity =
        snowflakesConnectorController.snowflakeUnloadToGCS(snowflakeUnloadToGCSRequestDTO);
    assertThat(responseEntity.getStatusCode().is2xxSuccessful()).isTrue();

    // Get the response body and cast to the expected type
    Object responseBody = responseEntity.getBody();
    Map<?, ?> responseMap = (Map<?, ?>) responseBody;
    Map<String, String> responseMapOutput = (Map<String, String>) responseMap;
    Assert.assertEquals("Success", responseMapOutput.get("Table1"));
  }

  /** This is the negative scenario where error is reproduced during the call execution. */
  @Test
  public void testSnowflakeUnloadToGCSError() {
    SnowflakeUnloadToGCSRequestDTO snowflakeUnloadToGCSRequestDTO =
        new SnowflakeUnloadToGCSRequestDTO();
    snowflakeUnloadToGCSRequestDTO.setSnowflakeStageLocation("");
    snowflakeUnloadToGCSRequestDTO.setSourceTableName("table1");
    snowflakeUnloadToGCSRequestDTO.setSnowflakeStageLocation(
        "snowflake-to-gcs-migration/data-unload");
    snowflakeUnloadToGCSRequestDTO.setSnowflakeFileFormatValue("SF_GCS_CSV_FORMAT1");
    CompletableFuture<OperationResult<String>> returnedResult =
        CompletableFuture.completedFuture(
            new OperationResult<>(new OperationResult.Error("Error processing GCS unload ")));
    when(snowflakeUnloadToGCSAsyncService.snowflakeUnloadToGCS(
            (any(SnowflakeUnloadToGCSDataDTO.class)), anyString()))
        .thenReturn(returnedResult);
    ResponseEntity<?> responseEntity =
        snowflakesConnectorController.snowflakeUnloadToGCS(snowflakeUnloadToGCSRequestDTO);
    assertThat(responseEntity.getStatusCode().is2xxSuccessful()).isTrue();

    // Get the response body and cast to the expected type
    Object responseBody = responseEntity.getBody();
    Map<?, ?> responseMap = (Map<?, ?>) responseBody;
    Map<String, String> responseMapOutput = (Map<String, String>) responseMap;
    Assert.assertEquals("Failed", responseMapOutput.get("Error processing GCS unload "));
  }

  @Test
  public void testProcessFailedRequest() {
    List<Long> requestIds = new ArrayList<>();
    requestIds.add(1L);
    List<SFDataMigrationResponse> sfDataMigrationResponseList = new ArrayList<>();
    SFDataMigrationResponse sfDataMigrationResponse = new SFDataMigrationResponse();
    sfDataMigrationResponse.setSourceTableName("test-table");
    sfDataMigrationResponse.setRequestLogId(UUID.randomUUID().toString());
    sfDataMigrationResponseList.add(sfDataMigrationResponse);

    when(snowflakeMigrateDataService.processFailedRequestMigratedRows()).thenReturn(requestIds);
    when(snowflakeMigrateDataService.getApplicationConfigDataByIds(any(List.class)))
        .thenReturn(sfDataMigrationResponseList);

    ResponseEntity<?> responseEntity = snowflakesConnectorController.processFailedRequest();
    assertThat(responseEntity.getStatusCode().is2xxSuccessful()).isTrue();

    // Get the response body and cast to the expected type
    Object responseBody = responseEntity.getBody();
    if (responseBody instanceof List<?>) {
      List<?> responseList = (List<?>) responseBody;
      // Verify the type of elements in the list if needed
      assertThat(responseList.get(0)).isInstanceOf(SFDataMigrationResponse.class);
      // Further assertions for a non-empty list
      List<SFDataMigrationResponse> longList = (List<SFDataMigrationResponse>) responseList;
      Assert.assertEquals("test-table", longList.get(0).getSourceTableName());
    } else {
      Assert.fail();
    }
  }

  @Test
  public void testProcessFailedRequestEmptyRequestIds() {
    List<Long> requestIds = new ArrayList<>();
    List<SFDataMigrationResponse> sfDataMigrationResponseList = new ArrayList<>();
    SFDataMigrationResponse sfDataMigrationResponse = new SFDataMigrationResponse();
    sfDataMigrationResponse.setSourceTableName("test-table");
    sfDataMigrationResponse.setRequestLogId(UUID.randomUUID().toString());
    sfDataMigrationResponseList.add(sfDataMigrationResponse);

    when(snowflakeMigrateDataService.processFailedRequestMigratedRows()).thenReturn(requestIds);
    when(snowflakeMigrateDataService.getApplicationConfigDataByIds(any(List.class)))
        .thenReturn(sfDataMigrationResponseList);

    ResponseEntity<?> responseEntity = snowflakesConnectorController.processFailedRequest();
    assertThat(responseEntity.getStatusCode().is2xxSuccessful()).isTrue();

    // Get the response body and cast to the expected type
    Object responseBody = responseEntity.getBody();
    Assert.assertEquals(
        "There are no failed requests that are eligible for reprocessing", responseBody);
  }

  @Test
  public void testProcessFailedRequestNoFailedRequestExists() {

    when(snowflakeMigrateDataService.processFailedRequestMigratedRows()).thenReturn(null);
    ResponseEntity<?> responseEntity = snowflakesConnectorController.processFailedRequest();
    assertThat(responseEntity.getStatusCode().is2xxSuccessful()).isFalse();
  }

  @Test
  public void testProcessExtractDDL() {

    when(extractAndTranslateDDLService.extractAndTranslateDDLs(
            any(SFExtractAndTranslateDDLRequestDTO.class)))
        .thenReturn("Extract & translate DDL request completed successfully at");
    ResponseEntity<?> responseEntity =
        snowflakesConnectorController.extractDDL(new SFExtractAndTranslateDDLRequestDTO());
    assertThat(responseEntity.getStatusCode().is2xxSuccessful()).isTrue();
    Assert.assertTrue(
        responseEntity
            .getBody()
            .toString()
            .contains("Extract & translate DDL request completed successfully at."));
  }

  @Test
  public void testRefreshOAuthToken() {
    when(tokenRefreshService.refreshToken()).thenReturn(new TokenResponse());
    snowflakesConnectorController.refreshOauthToken();
    Mockito.verify(tokenRefreshService, Mockito.times(1)).refreshToken();
  }

  @Test
  public void testEncryptValue() {
    Map<String, String> inputMap = new HashMap<>();
    inputMap.put("key", "unencryptvalue");
    when(encryptValues.encryptValue(anyString()))
        .thenReturn(
            new EncryptedData(
                "testencrypt",
                CommonMethods.generateSecretKey(),
                CommonMethods.generateInitializationVector()));
    Map<String, EncryptedData> returnedValue = snowflakesConnectorController.encryptValue(inputMap);
    Assert.assertEquals("testencrypt", returnedValue.get("key").getCiphertext());
  }

  @Test
  public void testEncryptAndSaveOAuthValue() {
    Map<String, String> inputMap = new HashMap<>();
    inputMap.put("key", "testencrypt");
    when(encryptValues.encryptValue(anyString()))
        .thenReturn(
            new EncryptedData(
                "testencrypt",
                CommonMethods.generateSecretKey(),
                CommonMethods.generateInitializationVector()));
    snowflakesConnectorController.encryptAndSaveOAuthValue(inputMap);
    Assert.assertEquals(
        inputMap.get("key"), oAuthCredentials.getOauthMap().get("key").getCiphertext());
  }
}
