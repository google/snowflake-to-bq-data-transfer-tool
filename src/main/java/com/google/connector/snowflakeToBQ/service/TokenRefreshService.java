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

import static com.google.connector.snowflakeToBQ.util.ErrorCode.TOKEN_REFRESH_ERROR;

import com.google.connector.snowflakeToBQ.config.OAuthCredentials;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.response.TokenResponse;
import com.google.connector.snowflakeToBQ.util.encryption.EncryptValues;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

/** Class to help in refreshing the snowflakes oauth token. */
@Service
@Setter
public class TokenRefreshService {
  private static final Logger log = LoggerFactory.getLogger(TokenRefreshService.class);
  private final OAuthCredentials oauthCredentials;
  private final RestTemplate restTemplate;
  private final EncryptValues encryptDecryptValues;

  @Value("${snowflake.account.url}")
  @Setter
  String snowflakeAccountURL;

  public TokenRefreshService(
      OAuthCredentials oauthCredentials,
      RestTemplate restTemplate,
      EncryptValues encryptDecryptValues) {
    this.oauthCredentials = oauthCredentials;
    this.restTemplate = restTemplate;
    this.encryptDecryptValues = encryptDecryptValues;
  }

  /**
   * This method reads oauth related properties from application.properties and execute the rest
   * call to refresh the Oauth token using the refresh token. It basically fetches the access token
   * based on the refresh token
   *
   * @return @{@link TokenResponse}
   */
  public TokenResponse refreshToken() {

    if (oauthCredentials.getOauthMap().size() == 0) {
      log.info(
          "OAUTH related information is not yet set, please set it before starting the rest API execution");
      return null;
    }

    // Build the request URL and body
    MultiValueMap<String, String> requestBody = new LinkedMultiValueMap<>();
    String refreshToken =
        encryptDecryptValues.decryptValue(oauthCredentials.getOauthMap().get("refreshToken"));
    String clientId =
        encryptDecryptValues.decryptValue(oauthCredentials.getOauthMap().get("clientId"));
    String clientSecret =
        encryptDecryptValues.decryptValue(oauthCredentials.getOauthMap().get("clientSecret"));
    requestBody.add("grant_type", "refresh_token");
    requestBody.add("refresh_token", refreshToken);
    requestBody.add("client_id", clientId);
    requestBody.add("client_secret", clientSecret);

    // Send the request to refresh the token and retrieve the response contains access token
    try {
      TokenResponse response =
          restTemplate.postForObject(
              snowflakeAccountURL + "/oauth/token-request", requestBody, TokenResponse.class);
      if (response == null) {
        throw new SnowflakeConnectorException(
            TOKEN_REFRESH_ERROR.getMessage(), TOKEN_REFRESH_ERROR.getErrorCode());
      }
      // Setting the encrypted OAuth token
      oauthCredentials
          .getOauthMap()
          .put("accessToken", encryptDecryptValues.encryptValue(response.getAccessToken()));
      return response;
    } catch (Exception e) {
      log.error("Error while refreshing the token:{}", e.getMessage());
      throw new SnowflakeConnectorException(e.getMessage(), TOKEN_REFRESH_ERROR.getErrorCode());
    }
  }
}
