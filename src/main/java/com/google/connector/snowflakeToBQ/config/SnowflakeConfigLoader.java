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

package com.google.connector.snowflakeToBQ.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

/**
 * This class helps in creating a bean which loads the files and data related to Snowflake migration
 * request execution. Its reads the path from property where the actual file is expected to present.
 * It helps accepting the file at runtime with user defined values.
 */
@Configuration
public class SnowflakeConfigLoader {
  private static final Logger log = LoggerFactory.getLogger(SnowflakeConfigLoader.class);

  @Value("${snowflake.table.query.mapping.path}")
  private String snowflakeTableQueryMappingPath;

  @Value("${snowflake.request.body.json.path}")
  private String snowflakeRequestBodyJSONPath;

  private Map<String, String> snowflakeTableAndQuery;

  private Map<String, String> snowflakeUnloadDataRequestBody;

  private final ResourceLoader resourceLoader;

  public SnowflakeConfigLoader(ResourceLoader resourceLoader) {
    this.resourceLoader = resourceLoader;
  }

  /**
   * This method load the JSON file present at the path given to the property
   * snowflakeTableQueryMappingPath. JSON file contains the key value pair where key is the table
   * name and value will be the select query for the table. This query will help exporting the data
   * from Snowflakes based on the query rather than full table export. This is needed for few of the
   * cases like 1: File format(CSV, Parquet) does not support the specific column of Snowflake. 2:
   * We need to change any format or value of column, e.g. we need to give date in different format
   * then what is set in BigQuery. 3: Table has PII, PHI column, and we * don't want to bring that
   * Data.
   *
   * <p>This class loads that file and create a map of the JSON value. This map gets referred at the
   * time of actual data extracted with in the code.
   */
  @Bean
  public void loadSnowflakeTableQueryMapping() {

    try {
      ObjectMapper objectMapper = new ObjectMapper();

      log.info(
          "File path for Snowflake table and query mapping file:{}",
          snowflakeTableQueryMappingPath);
      Resource resource = getResourceHelper(snowflakeTableQueryMappingPath);

      snowflakeTableAndQuery =
          objectMapper.readValue(resource.getInputStream(), new TypeReference<>() {});

      for (Map.Entry<String, String> entry : snowflakeTableAndQuery.entrySet()) {
        log.info("Snowflake Table and Query mapping details ====>");
        log.info("Table Name :{} ", entry.getKey());
        log.info("Query:{} " + entry.getValue());
      }
    } catch (Exception e) {
      log.error(
          "Error while loading the file contains the table name and query from the path :{}",
          snowflakeTableQueryMappingPath);
      log.error("Error Message::{}\nStack Trace:", e.getMessage(), e);
      throw new SnowflakeConnectorException(
          ErrorCode.SNOWFLAKE_CONFIG_LOADER.getMessage(),
          ErrorCode.SNOWFLAKE_CONFIG_LOADER.getErrorCode());
    }
  }

  /**
   * This method load the JSON file given at the path defined by variable
   * snowflakeRequestBodyJSONPath. File basically contains the request body of different requests.
   * This method loads the file and create a map of request and its body which gets used by
   * application.
   */
  @Bean
  public void loadSnowflakeRequestBody() {

    try {
      ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

      log.info("File path for Snowflake request body JSON file:{}", snowflakeRequestBodyJSONPath);

      Resource resource = getResourceHelper(snowflakeRequestBodyJSONPath);
      Map<String, String> inputValue = objectMapper.readValue(resource.getInputStream(), Map.class);
      snowflakeUnloadDataRequestBody = new HashMap<>();

      for (Map.Entry<String, String> entry : inputValue.entrySet()) {
        log.info("Snowflake request body mapping details ====>");

        snowflakeUnloadDataRequestBody.put(
            entry.getKey(), objectMapper.writeValueAsString(entry.getValue()));
        log.info("Request name :{} ", entry.getKey());
        log.info("Request body:{} " + snowflakeUnloadDataRequestBody.get(entry.getKey()));
      }
    } catch (Exception e) {
      log.error(
          "Error while loading the file contains Snowflake unload data request from the path :{}\nStack Trace:",
          snowflakeRequestBodyJSONPath,
          e);
      log.error("Error Message::{}", e.getMessage());
      throw new SnowflakeConnectorException(
          ErrorCode.SNOWFLAKE_CONFIG_LOADER.getMessage(),
          ErrorCode.SNOWFLAKE_CONFIG_LOADER.getErrorCode());
    }
  }

  /**
   * Gives the query associated with the table.
   *
   * @param tableName name of table which has any query associated for data extraction.
   * @return query against the table name
   */
  public String getQuery(String tableName) {
    return snowflakeTableAndQuery.get(tableName);
  }

  /**
   * Gives the request body associated with the request name.
   *
   * @param requestName name of request, its just name given by this application rather than a
   *     public request name.
   * @return request body for the request
   */
  public String getSnowflakeUnloadRequestBody(String requestName) {
    return snowflakeUnloadDataRequestBody.get(requestName);
  }

  /**
   * Helper method to load the resource based on location either from classpath or from file path.
   *
   * @param resourcePath path of the resource to be loaded.
   * @return @{@link Resource} object
   */
  private Resource getResourceHelper(String resourcePath) {
    Resource resource;
    if (resourcePath.startsWith("classpath:")) {
      resource = new ClassPathResource(resourcePath.substring("classpath:".length()));
    } else {
      resource = new FileSystemResource(resourcePath);
    }
    return resource;
  }
}
