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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.config.OAuthCredentials;
import com.google.connector.snowflakeToBQ.model.request.SFDataMigrationRequestDTO;
import com.google.connector.snowflakeToBQ.model.response.SFDataMigrationResponse;
import com.google.connector.snowflakeToBQ.model.response.TokenResponse;
import com.google.connector.snowflakeToBQ.service.SnowflakeMigrateDataService;
import com.google.connector.snowflakeToBQ.service.TokenRefreshService;
import com.google.connector.snowflakeToBQ.util.encryption.EncryptValues;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

public class SnowflakesConnectorControllerTest extends AbstractTestBase {

  @Autowired SnowflakesConnectorController snowflakesConnectorController;
  @Autowired OAuthCredentials oAuthCredentials;

  @MockBean EncryptValues encryptValues;
  @MockBean TokenRefreshService tokenRefreshService;
  @MockBean SnowflakeMigrateDataService snowflakeMigrateDataService;

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
  public void testRefreshOAuthToken() {
    when(tokenRefreshService.refreshToken()).thenReturn(new TokenResponse());
    snowflakesConnectorController.refreshOauthToken();
    Mockito.verify(tokenRefreshService, Mockito.times(1)).refreshToken();
  }

  @Test
  public void testEncryptValue() {
    Map<String, String> inputMap = new HashMap<>();
    inputMap.put("key", "unencryptvalue");
    when(encryptValues.encryptValue(anyString())).thenReturn("testencrypt");
    Map<String, String> returnedValue = snowflakesConnectorController.encryptValue(inputMap);
    Assert.assertEquals("testencrypt", returnedValue.get("key"));
  }

  @Test
  public void testEncryptAndSaveOAuthValue() {
    Map<String, String> inputMap = new HashMap<>();
    inputMap.put("key", "testencrypt");
    when(encryptValues.encryptValue(anyString())).thenReturn("testencrypt");
    snowflakesConnectorController.encryptAndSaveOAuthValue(inputMap);
    Assert.assertEquals(inputMap, oAuthCredentials.getOauthMap());
  }
}
