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

package com.google.connector.snowflakeToBQ.repository;

import static com.google.connector.snowflakeToBQ.util.ErrorCode.JDBC_EXECUTION_EXCEPTION;

import com.google.connector.snowflakeToBQ.config.OAuthCredentials;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.service.TokenRefreshService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.connector.snowflakeToBQ.util.encryption.EncryptValues;
import lombok.Setter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Class to execute queries on snowflake database using jdbc template. It uses for connection
 * related information.
 */
@Repository
@Setter
public class SnowflakesJdbcDataRepository {
  private static final Logger log = LoggerFactory.getLogger(SnowflakesJdbcDataRepository.class);
  private static final String GET_DDL_QUERY = "select GET_DDL('TABLE', '%s')";
  private static final String SHOW_TABLES_QUERY = "SHOW TABLES IN SCHEMA %s";
  private final TokenRefreshService tokenRefreshService;
  private final OAuthCredentials oauthCredentials;

  @Value("${jdbc.url}")
  private String url;

  @Value("${authenticator.type}")
  private String authenticatorType;

  private JdbcTemplate jdbcTemplate;

  private final EncryptValues encryptDecryptValues;

  @Autowired
  public SnowflakesJdbcDataRepository(
      TokenRefreshService tokenRefreshService,
      OAuthCredentials oauthCredentials,
      EncryptValues encryptDecryptValues) {
    this.tokenRefreshService = tokenRefreshService;
    this.oauthCredentials = oauthCredentials;
    this.encryptDecryptValues = encryptDecryptValues;
  }

  /**
   * Setting up the JDBC template used for extracting the DDLs. Since the OAuth-related values are
   * provided by the user, they cannot be configured during application startup. In this context, a
   * null check is performed for the initial call after the user has configured the OAuth
   * credentials.
   *
   * @param databaseName Name of the database in Snowflake
   * @param schemaName name of the schema
   */
  public void setJdbcTemplate(String databaseName, String schemaName) {
    if (jdbcTemplate == null) {
      if (oauthCredentials.getOauthMap().get("accessToken") == null
          || StringUtils.isEmpty(
              oauthCredentials.getOauthMap().get("accessToken").getCiphertext())) {
        tokenRefreshService.refreshToken();
      }
      BasicDataSource dataSource = new BasicDataSource();
      dataSource.setUrl(url);
      dataSource.addConnectionProperty("db", databaseName);
      dataSource.addConnectionProperty("schema", schemaName);
      dataSource.addConnectionProperty("authenticator", authenticatorType);
      dataSource.addConnectionProperty(
          "token",
          encryptDecryptValues.decryptValue(oauthCredentials.getOauthMap().get("accessToken")));
      this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
  }

  /**
   * Extracting the DDLs of the given table
   *
   * @param tableName name of table for which DDLS need to be extracted.
   * @return DDLs in string format
   */
  public String extractDDL(String tableName) {
    String sql = String.format(GET_DDL_QUERY, tableName);
    String result = "";
    try {
      result = jdbcTemplate.queryForObject(sql, String.class);
    } catch (Exception e) {
      log.error(JDBC_EXECUTION_EXCEPTION.getMessage() + ", Error Message:{}", e.getMessage());
      throw new SnowflakeConnectorException(
          JDBC_EXECUTION_EXCEPTION.getMessage(), JDBC_EXECUTION_EXCEPTION.getErrorCode());
    }
    log.info("Table Name:{}, Extracted DDL From Snowflakes: {}", tableName, result);
    return result;
  }

  /**
   * Getting the tables present in the schema
   *
   * @param schema schema in the database
   * @return List of Table names
   */
  public List<String> getAllTableNames(String schema) {
    String query = String.format(SHOW_TABLES_QUERY, schema);

    try {
      return jdbcTemplate.query(query, (rs, rowNum) -> rs.getString("name"));
    } catch (Exception e) {
      log.error("Error while obtaining the jdbc connection, Error Message:{}", e.getMessage());
      throw new SnowflakeConnectorException(
          JDBC_EXECUTION_EXCEPTION.getMessage(), JDBC_EXECUTION_EXCEPTION.getErrorCode());
    }
  }

  /**
   * Extract the ddls of all the tables present in the Schema
   *
   * @param schema schema name in the database
   * @return Map of table name and corresponding ddls.
   */
  public Map<String, String> getAllTableDDLs(String schema) {
    Map<String, String> tableMap = new HashMap<>();
    List<String> tableNames = getAllTableNames(schema);
    for (String tableName : tableNames) {
      tableMap.put(tableName, extractDDL(tableName));
    }
    return tableMap;
  }

  /**
   * Extract the ddls of all the tables given in the array
   *
   * @param tableNameValues name of tables for which DDls needs to be extracted.
   * @return Map of table name and corresponding ddls.
   */
  public Map<String, String> multipleTableDDLS(String[] tableNameValues) {
    Map<String, String> tableMap = new HashMap<>();
    for (String tableName : tableNameValues) {
      tableMap.put(tableName, extractDDL(tableName));
    }
    return tableMap;
  }
}
