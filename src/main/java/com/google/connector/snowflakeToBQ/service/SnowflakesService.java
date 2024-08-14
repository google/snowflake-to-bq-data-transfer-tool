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

import com.google.connector.snowflakeToBQ.config.SnowflakeConfigLoader;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.SnowflakeUnloadToGCSDataDTO;
import com.google.connector.snowflakeToBQ.model.response.SnowflakeResponse;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

import static com.google.connector.snowflakeToBQ.util.PropertyManager.SNOWFLAKE_STATEMENT_POST_REST_API;

@Service
public class SnowflakesService {
  private static final Logger log = LoggerFactory.getLogger(SnowflakesService.class);
  final RestAPIExecutionService restService;
  final SnowflakeConfigLoader snowflakeConfigLoader;

  @Value("${snowflake.account.url}")
  @Setter
  @Getter
  String snowflakeAccountURl;

  @Value("${gcs.storage.integration}")
  @Setter
  @Getter
  String gcsStorageIntegration;

  public SnowflakesService(
      RestAPIExecutionService restService, SnowflakeConfigLoader snowflakeConfigLoader) {
    this.restService = restService;
    this.snowflakeConfigLoader = snowflakeConfigLoader;
  }

  public String executeUnloadDataCommand(SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO) {

    String command =
        resolvePlaceholders(
            snowflakeConfigLoader.getSnowflakeUnloadRequestBody("UnloadDataRequest"),
            getPlaceHoldersMap(snowflakeUnloadToGCSDataDTO));
    log.info("Snowflake Command to be executed from Rest API:{}", command);

    SnowflakeResponse response =
        restService
            .executePostAndPoll(snowflakeAccountURl + SNOWFLAKE_STATEMENT_POST_REST_API, command)
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
              snowflakeAccountURl + SNOWFLAKE_STATEMENT_POST_REST_API,
              response.getStatementHandle());
      log.info(
          "Snowflake polling statement handle command rest API execution result:{}",
          pollReturnValue);
      if (!pollReturnValue) {
        throw new SnowflakeConnectorException(
            ErrorCode.SNOWFLAKE_REST_API_POLL_ERROR.getMessage(),
            ErrorCode.SNOWFLAKE_REST_API_POLL_ERROR.getErrorCode());
      }
    }
    log.info(
        "Copy into command successfully executed for table :{}",
        snowflakeUnloadToGCSDataDTO.getTableName());
    return response.getStatementHandle();
  }

  /**
   * Replaces placeholders in the given content with values from the provided map.
   *
   * <p>This method looks for placeholders in the format "{{PLACEHOLDER_NAME}}" within the provided
   * content string and replaces them with the corresponding values from the map. If a placeholder
   * is not found in the map, it is left unchanged in the content.
   *
   * @param content The string content containing placeholders to be replaced. The placeholders
   *     should be in the format "{{PLACEHOLDER_NAME}}".
   * @param values A map containing placeholder names and their corresponding replacement values.
   *     The keys in the map should match the placeholder names without the surrounding curly
   *     braces.
   * @return The content string with placeholders replaced by their corresponding values from the
   *     map.
   */
  private String resolvePlaceholders(String content, Map<String, String> values) {
    for (Map.Entry<String, String> entry : values.entrySet()) {
      content = content.replace("{{" + entry.getKey() + "}}", entry.getValue());
    }
    return content;
  }

  /**
   * Method to return placeholders map which contains all the placeholder which will get replaced
   * with the value in the snowflake_request_body.json. Key of the map is actual place holders and
   * value of the map is the replacement value.
   *
   * @param snowflakeUnloadToGCSDataDTO dto containing data related to Snowflake unload request.
   * @return @{@link Map} of place holders.
   */
  private Map<String, String> getPlaceHoldersMap(
      SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO) {
    Map<String, String> placeHolders = new HashMap<>();

    placeHolders.put("WAREHOUSE", snowflakeUnloadToGCSDataDTO.getWarehouse());
    placeHolders.put("TABLE_NAME", snowflakeUnloadToGCSDataDTO.getTableName());
    placeHolders.put("DATABASE", snowflakeUnloadToGCSDataDTO.getDatabaseName());
    placeHolders.put("SCHEMA", snowflakeUnloadToGCSDataDTO.getSchemaName());
    placeHolders.put("STAGE_LOCATION", snowflakeUnloadToGCSDataDTO.getSnowflakeStageLocation());
    placeHolders.put("FILE_FORMAT", snowflakeUnloadToGCSDataDTO.getSnowflakeFileFormatValue());
    placeHolders.put("STORAGE_INTEGRATION", gcsStorageIntegration);

    String snowflakeQuery = snowflakeUnloadToGCSDataDTO.getTableName();
    if (!StringUtils.isEmpty(
        snowflakeConfigLoader.getQuery(snowflakeUnloadToGCSDataDTO.getTableName()))) {
      snowflakeQuery =
          "(" + snowflakeConfigLoader.getQuery(snowflakeUnloadToGCSDataDTO.getTableName()) + ")";
    }
    placeHolders.put("SNOWFLAKE_QUERY", snowflakeQuery);
    return placeHolders;
  }
}
