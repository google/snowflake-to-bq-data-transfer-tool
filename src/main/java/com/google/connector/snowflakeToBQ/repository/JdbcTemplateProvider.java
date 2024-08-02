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

package com.google.connector.snowflakeToBQ.repository;

import com.google.connector.snowflakeToBQ.cache.EasyCache;
import com.google.connector.snowflakeToBQ.config.OAuthCredentials;
import com.google.connector.snowflakeToBQ.service.TokenRefreshService;
import com.google.connector.snowflakeToBQ.util.encryption.EncryptValues;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Provides and manages JDBC templates for interacting with various databases.
 *
 * <p>This class is responsible for creating and caching JDBC templates used for executing queries
 * on a specified database. It handles the dynamic configuration of the JDBC data source based on
 * the provided database and schema names. Additionally, it manages authentication tokens and their
 * encryption, ensuring that tokens are securely refreshed and utilized.
 *
 * <p>The JDBC templates are cached to optimize performance and reduce the overhead of repeatedly
 * creating data sources. If a required JDBC template is not found in the cache, a new one is
 * created with the appropriate database, schema, and authentication configurations. The class also
 * ensures that the authentication token is refreshed and decrypted as needed.
 *
 * <p>Note: Authentication-related values are provided by the user and cannot be configured during
 * application startup. Therefore, token refreshes and decryption are handled as part of the
 * template creation process.
 */
@Repository
@Setter
public class JdbcTemplateProvider {

  private static final Logger log = LoggerFactory.getLogger(JdbcTemplateProvider.class);

  private final TokenRefreshService tokenRefreshService;
  private final OAuthCredentials oauthCredentials;
  private final EncryptValues encryptDecryptValues;
  private final EasyCache<String, JdbcTemplate> jdbcTemplateCache;

  @Value("${jdbc.url}")
  private String url;

  @Value("${authenticator.type}")
  private String authenticatorType;

  @Autowired
  public JdbcTemplateProvider(
      TokenRefreshService tokenRefreshService,
      OAuthCredentials oauthCredentials,
      EncryptValues encryptDecryptValues,
      EasyCache<String, JdbcTemplate> jdbcTemplateCache) {
    this.tokenRefreshService = tokenRefreshService;
    this.oauthCredentials = oauthCredentials;
    this.encryptDecryptValues = encryptDecryptValues;
    this.jdbcTemplateCache = jdbcTemplateCache;
  }

  /**
   * Retrieves or creates a JDBC template for the specified database and schema.
   *
   * <p>This method checks the cache for an existing {@link JdbcTemplate} for the given database
   * name. If one is not found, a new {@link JdbcTemplate} is created with the provided database and
   * schema configurations. The method also ensures that the authentication token is refreshed and
   * decrypted before it is used to configure the JDBC data source.
   *
   * <p>If the OAuth access token is missing or invalid, it will be refreshed using the {@link
   * TokenRefreshService} and decrypted using the {@link EncryptValues} class.
   *
   * @param databaseName the name of the database in the target database system.
   * @param schemaName the name of the schema within the database.
   * @return a {@link JdbcTemplate} configured for the specified database and schema.
   */
  public JdbcTemplate getOrCreateJdbcTemplate(String databaseName, String schemaName) {
    String cacheKey = databaseName + schemaName;
    JdbcTemplate jdbcTemplate = jdbcTemplateCache.get(cacheKey);
    // Create and cache a new JdbcTemplate if not found in cache
    if (jdbcTemplate == null) {
      HikariDataSource dataSource =
          DataSourceBuilder.create()
              .type(HikariDataSource.class)
              .url(url)
              .driverClassName("net.snowflake.client.jdbc.SnowflakeDriver")
              .build();

      dataSource.addDataSourceProperty("db", databaseName);
      dataSource.addDataSourceProperty("schema", schemaName);
      dataSource.addDataSourceProperty("authenticator", authenticatorType);
      dataSource.setMinimumIdle(1);
      dataSource.setMaximumPoolSize(2);
      if (oauthCredentials.getOauthMap().get("accessToken") == null
          || StringUtils.isEmpty(
              oauthCredentials.getOauthMap().get("accessToken").getCiphertext())) {
        tokenRefreshService.refreshToken();
      }
      String decryptedToken =
          encryptDecryptValues.decryptValue(oauthCredentials.getOauthMap().get("accessToken"));
      dataSource.addDataSourceProperty("token", decryptedToken);
      jdbcTemplate = new JdbcTemplate(dataSource);

      jdbcTemplateCache.put(cacheKey, jdbcTemplate);
    }

    return jdbcTemplate;
  }
}
