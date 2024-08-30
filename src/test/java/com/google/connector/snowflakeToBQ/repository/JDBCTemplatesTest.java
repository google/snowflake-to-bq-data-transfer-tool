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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.cache.EasyCache;
import com.google.connector.snowflakeToBQ.config.OAuthCredentials;
import com.google.connector.snowflakeToBQ.model.EncryptedData;
import com.google.connector.snowflakeToBQ.model.response.TokenResponse;
import com.google.connector.snowflakeToBQ.service.TokenRefreshService;
import com.google.connector.snowflakeToBQ.util.encryption.EncryptValues;
import com.zaxxer.hikari.HikariDataSource;
import java.util.*;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;

public class JDBCTemplatesTest extends AbstractTestBase {

  @Autowired JdbcTemplateProvider jdbcTemplates;
  @MockBean TokenRefreshService tokenRefreshService;
  @MockBean OAuthCredentials oauthCredentials;
  @MockBean EasyCache<String, ClosableJdbcTemplate> jdbcTemplateCache;

  @MockBean EncryptValues encryptDecryptValues;

  @Mock ClosableJdbcTemplate template;

  @Test
  public void testSetJdbcTemplateCacheHasNoData() {
    String databaseName = "new_database";
    String schemaName = "test_schema";

    when(jdbcTemplateCache.get(eq(databaseName + schemaName))).thenReturn(null);
    EncryptedData encryptedDataMock = mock(EncryptedData.class);
    Map mapMock = mock(HashMap.class);
    when(oauthCredentials.getOauthMap()).thenReturn(mapMock);
    when(mapMock.get("accessToken")).thenReturn(null).thenReturn(encryptedDataMock);

    when(tokenRefreshService.refreshToken()).thenReturn(new TokenResponse());
    when(encryptDecryptValues.decryptValue(any(EncryptedData.class))).thenReturn("decrept-token");

    // Call the method under test. Below conditions are true during execution as per mock setting
    // oauthCredentials.getOauthMap().get("accessToken") == null =true
    // StringUtils.isEmpty( oauthCredentials.getOauthMap().get("accessToken").getCiphertext()) =true
    JdbcTemplate jdbcTemplate = jdbcTemplates.getOrCreateJdbcTemplate(databaseName, schemaName);
    Assert.assertNotNull(jdbcTemplate);
    Assert.assertEquals(2, ((HikariDataSource) jdbcTemplate.getDataSource()).getMaximumPoolSize());
  }

  @Test
  public void testSetJdbcTemplateCacheHasNoDataScenario1() {
    String databaseName = "new_database";
    String schemaName = "test_schema";

    when(jdbcTemplateCache.get(eq(databaseName + schemaName))).thenReturn(null);
    EncryptedData encryptedDataMock = mock(EncryptedData.class);
    when(encryptedDataMock.getCiphertext()).thenReturn("access-token");

    Map mapMock = mock(HashMap.class);
    when(oauthCredentials.getOauthMap()).thenReturn(mapMock);
    // Making the first part of the if in main method return false and second path to true
    when(mapMock.get("accessToken")).thenReturn(null).thenReturn(encryptedDataMock);
    when(encryptDecryptValues.decryptValue(any(EncryptedData.class))).thenReturn("decrept-token");

    // Call the method under test
    // Call the method under test. Below conditions are true/false during execution as per mock setting
    // oauthCredentials.getOauthMap().get("accessToken") == null =true
    // StringUtils.isEmpty( oauthCredentials.getOauthMap().get("accessToken").getCiphertext()) =false

    JdbcTemplate jdbcTemplate = jdbcTemplates.getOrCreateJdbcTemplate(databaseName, schemaName);
    Assert.assertNotNull(jdbcTemplate);
    Assert.assertEquals(2, ((HikariDataSource) jdbcTemplate.getDataSource()).getMaximumPoolSize());
    Assert.assertEquals("decrept-token", ((HikariDataSource) jdbcTemplate.getDataSource()).getDataSourceProperties().get("token"));
  }

  @Test
  public void testSetJdbcTemplateCacheHasNoDataOauthRefreshScenario() {
    String databaseName = "new_database";
    String schemaName = "test_schema";

    when(jdbcTemplateCache.get(eq(databaseName + schemaName))).thenReturn(null);
    EncryptedData encryptedDataMock = mock(EncryptedData.class);
    when(encryptedDataMock.getCiphertext()).thenReturn("access-token");

    Map mapMock = mock(HashMap.class);
    // setting the first part of the if condition in main method true
    when(oauthCredentials.getOauthMap()).thenReturn(mapMock);
    when(mapMock.get("accessToken")).thenReturn(encryptedDataMock);
    // setting ciphertext to empty to make the second part of the if condition in main
    // method false
    when(encryptedDataMock.getCiphertext()).thenReturn("");
    when(tokenRefreshService.refreshToken()).thenReturn(new TokenResponse());
    when(encryptDecryptValues.decryptValue(any(EncryptedData.class))).thenReturn("decrept-token");

    // Call the method under test. Below conditions are true/false during execution as per mock setting
    // oauthCredentials.getOauthMap().get("accessToken") == null =false
    // StringUtils.isEmpty( oauthCredentials.getOauthMap().get("accessToken").getCiphertext()) =true
    JdbcTemplate jdbcTemplate = jdbcTemplates.getOrCreateJdbcTemplate(databaseName, schemaName);
    Assert.assertNotNull(jdbcTemplate);
    Assert.assertEquals(2, ((HikariDataSource) jdbcTemplate.getDataSource()).getMaximumPoolSize());
    Assert.assertEquals("decrept-token", ((HikariDataSource) jdbcTemplate.getDataSource()).getDataSourceProperties().get("token"));
  }

  @Test
  public void testSetJdbcTemplateCacheHasNoDataOauthRefreshScenario1() {
    String databaseName = "new_database";
    String schemaName = "test_schema";

    when(jdbcTemplateCache.get(eq(databaseName + schemaName)))
            .thenReturn(null); // Simulate cache miss
    EncryptedData encryptedDataMock = mock(EncryptedData.class);

    Map mapMock = mock(HashMap.class);
    // setting the first part of the if condition in main method true
    when(oauthCredentials.getOauthMap()).thenReturn(mapMock);
    when(mapMock.get("accessToken")).thenReturn(encryptedDataMock);
    // setting ciphertext to return some value to make the second part of the if condition in main
    // method true
    when(encryptedDataMock.getCiphertext()).thenReturn("access-token-value");

    when(tokenRefreshService.refreshToken()).thenReturn(new TokenResponse());
    when(encryptDecryptValues.decryptValue(any(EncryptedData.class))).thenReturn("decrept-token");

    // Call the method under test. Below conditions are false during execution as per mock setting
    // oauthCredentials.getOauthMap().get("accessToken") == null =false
    // StringUtils.isEmpty( oauthCredentials.getOauthMap().get("accessToken").getCiphertext()) =false
    JdbcTemplate jdbcTemplate = jdbcTemplates.getOrCreateJdbcTemplate(databaseName, schemaName);
    Assert.assertNotNull(jdbcTemplate);
    Assert.assertEquals("decrept-token", ((HikariDataSource) jdbcTemplate.getDataSource()).getDataSourceProperties().get("token"));
  }

  @Test
  public void testSetJdbcTemplateCacheHasData() {
    String databaseName = "new_database";
    String schemaName = "test_schema";
    try (HikariDataSource dataSource =
        DataSourceBuilder.create()
            .type(HikariDataSource.class)
            .url("test-url")
            .driverClassName(
                "net.snowflake.client.jdbc.SnowflakeDriver")
            .build()) {

      dataSource.addDataSourceProperty("db", databaseName);
      dataSource.addDataSourceProperty("schema", schemaName);
      dataSource.addDataSourceProperty("authenticator", "test");
      dataSource.setMinimumIdle(1);
      dataSource.setMaximumPoolSize(5);
      this.template = new ClosableJdbcTemplate(dataSource);
    }

    when(jdbcTemplateCache.get(eq(databaseName + schemaName))).thenReturn(template);
    JdbcTemplate jdbcTemplate = jdbcTemplates.getOrCreateJdbcTemplate(databaseName, schemaName);
    Assert.assertNotNull(jdbcTemplate);
    Assert.assertEquals("test-url", ((HikariDataSource) jdbcTemplate.getDataSource()).getJdbcUrl());
    Assert.assertEquals(5, ((HikariDataSource) jdbcTemplate.getDataSource()).getMaximumPoolSize());
  }
}
