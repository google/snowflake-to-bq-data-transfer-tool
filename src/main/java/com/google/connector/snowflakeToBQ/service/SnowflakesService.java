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

import static com.google.connector.snowflakeToBQ.util.PropertyManager.SNOWFLAKE_STATEMENT_POST_REST_API;

import com.google.connector.snowflakeToBQ.config.OAuthCredentials;
import com.google.connector.snowflakeToBQ.config.SnowflakeConfigLoader;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.response.SnowflakeResponse;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import com.google.connector.snowflakeToBQ.util.encryption.EncryptValues;
import lombok.Getter;
import lombok.Setter;
import net.snowflake.client.jdbc.internal.apache.tika.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SnowflakesService {
  private static final Logger log = LoggerFactory.getLogger(SnowflakesService.class);
  final RestAPIExecutionService restService;
  final SnowflakeConfigLoader snowflakeConfigLoader;
  private final OAuthCredentials oauthCredentials;
  private final EncryptValues encryptDecryptValues;
  private final TokenRefreshService tokenRefreshService;

  @Value("${snowflake.account.url}")
  @Setter
  @Getter
  String snowflakeAccountURl;

  public SnowflakesService(
      RestAPIExecutionService restService,
      SnowflakeConfigLoader snowflakeConfigLoader,
      OAuthCredentials oauthCredentials,
      EncryptValues encryptDecryptValues,
      TokenRefreshService tokenRefreshService) {
    this.restService = restService;
    this.snowflakeConfigLoader = snowflakeConfigLoader;
    this.oauthCredentials = oauthCredentials;
    this.encryptDecryptValues = encryptDecryptValues;
    this.tokenRefreshService = tokenRefreshService;
  }

  public String executeUnloadDataCommand(
      String tableName, String copyIntoGCSUnloadPath, String snowflakeFileFormatValue) {
    // TODO Remove this temporary logic
    String snowflakeQuery = tableName;
    if (!StringUtils.isEmpty(snowflakeConfigLoader.getQuery(tableName))) {
      snowflakeQuery = "(" + snowflakeConfigLoader.getQuery(tableName) + ")";
    }

    // Replacing the values in above string variable.
    String command =
        String.format(
            snowflakeConfigLoader.getSnowflakeUnloadRequestBody("UnloadDataRequest"),
            tableName,
            copyIntoGCSUnloadPath,
            tableName,
            snowflakeFileFormatValue,
            tableName,
            tableName,
            snowflakeQuery);
    log.info("Snowflake Command to be executed from Rest API:{}", command);

    if (StringUtils.isEmpty(oauthCredentials.getOauthMap().get("accessToken"))) {
      tokenRefreshService.refreshToken();
    }
    String accessToken =
        encryptDecryptValues.decryptValue(oauthCredentials.getOauthMap().get("accessToken"));

    SnowflakeResponse response =
        restService
            .executePostAndPoll(
                snowflakeAccountURl + SNOWFLAKE_STATEMENT_POST_REST_API, command, accessToken)
            .block();
    log.info("Snowflake Copy Into command rest API execution response:{}", response);

    if (response == null || StringUtils.isEmpty(response.getMessage())) {
      log.error("Response value is ::{}", response);
      throw new SnowflakeConnectorException(
          ErrorCode.SNOWFLAKE_UNLOAD_DATA_ERROR.getMessage(),
          ErrorCode.SNOWFLAKE_UNLOAD_DATA_ERROR.getErrorCode());
    }

    if (!response.getMessage().equals("Statement executed successfully.")) {
      boolean pollReturnValue =
          restService.pollWithTimeout(
              SNOWFLAKE_STATEMENT_POST_REST_API, response.getStatementHandle(), accessToken);
      log.info(
          "Snowflake polling statement handle command rest API execution result:{}",
          pollReturnValue);
      if (!pollReturnValue) {
        throw new SnowflakeConnectorException(
            ErrorCode.SNOWFLAKE_REST_API_POLL_ERROR.getMessage(),
            ErrorCode.SNOWFLAKE_REST_API_POLL_ERROR.getErrorCode());
      }
    }
    log.info("Copy into command successfully executed for table :{}", tableName);
    return response.getStatementHandle();
  }
}
