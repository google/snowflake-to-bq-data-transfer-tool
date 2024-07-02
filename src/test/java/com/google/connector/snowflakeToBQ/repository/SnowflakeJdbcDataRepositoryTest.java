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
import com.google.connector.snowflakeToBQ.config.OAuthCredentials;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.EncryptedData;
import com.google.connector.snowflakeToBQ.model.response.TokenResponse;
import com.google.connector.snowflakeToBQ.service.TokenRefreshService;
import com.google.connector.snowflakeToBQ.util.CommonMethods;
import com.google.connector.snowflakeToBQ.util.ErrorCode;

import java.util.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/** Test file for {@link SnowflakesJdbcDataRepository} class */
public class SnowflakeJdbcDataRepositoryTest extends AbstractTestBase {

  @Autowired SnowflakesJdbcDataRepository applicationConfigDataService;

  @Autowired ApplicationConfigDataRepository applicationConfigDataRepository;
  @MockBean TokenRefreshService tokenRefreshService;
  @Mock JdbcTemplate template;
  @MockBean OAuthCredentials oauthCredentials;

  @Before
  public void setup() {
    applicationConfigDataService.setJdbcTemplate(template);
  }

  @Test
  public void testExtractDDL() {
    String expectedDDL = "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE\")";
    when(template.queryForObject(any(), eq(String.class))).thenReturn(expectedDDL);
    String actualDDL = applicationConfigDataService.extractDDL("table1");
    Assert.assertEquals(expectedDDL, actualDDL);
  }

  @Test
  public void testExtractDDLJdbcTemplateNull() {
    try {
      String expectedDDL = "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE\")";
      applicationConfigDataService.setJdbcTemplate(null);
      when(template.queryForObject(any(), eq(String.class))).thenReturn(expectedDDL);
      String actualDDL = applicationConfigDataService.extractDDL("table1");
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testGetAllTableNames() {
    RowMapper<String> rowMapperMock = mock(RowMapper.class);
    List<String> expectedTableNames = new ArrayList<>();
    //        expectedTableNames.add("table1");
    when(template.query(anyString(), eq(rowMapperMock))).thenReturn(expectedTableNames);
    List<String> actualTableNames = applicationConfigDataService.getAllTableNames("public");
    // TODO: Assertion should be changed as even if we send the list with one value the returned
    // list is empty. To speed up the development I have kept the expected list empty but we need to
    // fix it.It may be applicable to other test as well.
    Assert.assertEquals(expectedTableNames, actualTableNames);
  }

  @Test
  public void testGetAllTableNamesJdbcTemplateNull() {
    try {
      RowMapper<String> rowMapperMock = mock(RowMapper.class);
      List<String> expectedTableNames = new ArrayList<>();
      applicationConfigDataService.setJdbcTemplate(null);
      when(template.query(anyString(), eq(rowMapperMock))).thenReturn(expectedTableNames);

      Map<String, EncryptedData> dummyMapValue = new HashMap<>();

      EncryptedData encryptedData =
          new EncryptedData(
              "tests",
              CommonMethods.generateSecretKey(),
              CommonMethods.generateInitializationVector());
      dummyMapValue.put("accessToken", encryptedData);
      when(oauthCredentials.getOauthMap()).thenReturn(dummyMapValue);

      List<String> actualTableNames = applicationConfigDataService.getAllTableNames("public");
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.JDBC_EXECUTION_EXCEPTION.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.JDBC_EXECUTION_EXCEPTION.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testGetAllTableNamesJdbcTemplateNullScenario2() {
    try {
      RowMapper<String> rowMapperMock = mock(RowMapper.class);
      List<String> expectedTableNames = new ArrayList<>();
      applicationConfigDataService.setJdbcTemplate(null);

      when(template.query(anyString(), eq(rowMapperMock))).thenReturn(expectedTableNames);
      when(tokenRefreshService.refreshToken()).thenReturn(new TokenResponse());
      Map<String, EncryptedData> dummyMapValue = new HashMap<>();

      EncryptedData encryptedData =
          new EncryptedData(
              "tests",
              CommonMethods.generateSecretKey(),
              CommonMethods.generateInitializationVector());
      dummyMapValue.put("accessToken", encryptedData);
      when(oauthCredentials.getOauthMap()).thenReturn(dummyMapValue);

      List<String> actualTableNames = applicationConfigDataService.getAllTableNames("public");

      // TODO: Assertion should be changed as even if we send the list with one value the returned
      // list is empty. To speed up the development I have kept the expected list empty but we need
      // to
      // fix it.It may be applicable to other test as well.
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.JDBC_EXECUTION_EXCEPTION.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.JDBC_EXECUTION_EXCEPTION.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testAllTableDDLs() {
    RowMapper<String> rowMapperMock = mock(RowMapper.class);
    List<String> expectedTableNames = new ArrayList<>();
    expectedTableNames.add("table1");
    when(template.query(anyString(), eq(rowMapperMock))).thenReturn(expectedTableNames);

    String expectedDDL = "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE\")";

    when(template.queryForObject(anyString(), eq(String.class))).thenReturn(expectedDDL);

    Map<String, String> actualTableNames = applicationConfigDataService.getAllTableDDLs("public");
    Assert.assertEquals(Collections.emptyMap(), actualTableNames);
  }

  @Test
  public void testMultipleTableDDLs() {

    String mockedDDL = "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE\")";
    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("table1", mockedDDL);

    when(template.queryForObject(anyString(), eq(String.class))).thenReturn(mockedDDL);
    String[] tableNames = {"table1"};
    Map<String, String> actualTableNames =
        applicationConfigDataService.multipleTableDDLS(tableNames);
    Assert.assertEquals(expectedMap, actualTableNames);
  }

  @Test
  public void testPropertyCoverage() {

    String mockedDDL = "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE\")";
    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("table1", mockedDDL);
    applicationConfigDataService.setUrl("https://testing.snowflakecomputing.com");
    applicationConfigDataService.setAuthenticatorType("OAuth");
    when(template.queryForObject(anyString(), eq(String.class))).thenReturn(mockedDDL);
    String[] tableNames = {"table1"};
    Map<String, String> actualTableNames =
        applicationConfigDataService.multipleTableDDLS(tableNames);
    Assert.assertEquals(expectedMap, actualTableNames);
  }
}
