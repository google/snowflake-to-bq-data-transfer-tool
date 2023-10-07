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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.config.OAuthCredentials;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.response.TokenResponse;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import com.google.connector.snowflakeToBQ.util.encryption.EncryptValues;
import java.util.HashMap;
import java.util.Map;
import lombok.Setter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.web.client.RestTemplate;

@Setter
public class TokenRefreshServiceTest extends AbstractTestBase {

  @Value("${snowflake.account.url}")
  String snowflakeAccountURl;

  private String refreshToken;
  private TokenRefreshService tokenRefreshService;
  @MockBean private OAuthCredentials oauthCredentials;
  @MockBean private RestTemplate restTemplate;
  @MockBean private EncryptValues encryptDecryptValues;

  @Before
  public void setup() {
    tokenRefreshService =
        new TokenRefreshService(oauthCredentials, restTemplate, encryptDecryptValues);
    tokenRefreshService.setSnowflakeAccountURL(snowflakeAccountURl);
  }

  @Test
  public void testTokenResponseIsNull() {
    Map<String, String> dummyMapValue = new HashMap<>();
    dummyMapValue.put("refreshToken", "test-token");
    dummyMapValue.put("clientId", "client-id");
    dummyMapValue.put("clientSecret", "client-secret");
    when(oauthCredentials.getOauthMap()).thenReturn(dummyMapValue);
    when(restTemplate.postForObject(anyString(), anyString(), eq(TokenResponse.class)))
        .thenReturn(null);
    try {
      TokenResponse expectedResponse = tokenRefreshService.refreshToken();
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(e.getMessage(), ErrorCode.TOKEN_REFRESH_ERROR.getMessage());
      Assert.assertEquals(e.getErrorCode(), ErrorCode.TOKEN_REFRESH_ERROR.getErrorCode());
    }
  }

  @Test
  public void testGetAccessToken() {
    Map<String, String> dummyMapValue = new HashMap<>();
    dummyMapValue.put("refreshToken", "test-token");
    dummyMapValue.put("clientId", "client-id");
    dummyMapValue.put("clientSecret", "client-secret");
    when(oauthCredentials.getOauthMap()).thenReturn(dummyMapValue);
    TokenResponse tokenResponse = new TokenResponse();
    tokenResponse.setAccessToken("accessToken");
    tokenResponse.setExpiresInSeconds(1000);
    tokenResponse.setRefreshToken("DDbqHmHaQUB8cVW0CjCqTe");
    when(restTemplate.postForObject(anyString(), any(Object.class), eq(TokenResponse.class)))
        .thenReturn(tokenResponse);
    TokenResponse expectedResponse = tokenRefreshService.refreshToken();
    Assert.assertEquals(tokenResponse.getAccessToken(), expectedResponse.getAccessToken());
    Assert.assertEquals(tokenResponse.getRefreshToken(), expectedResponse.getRefreshToken());
  }
}
