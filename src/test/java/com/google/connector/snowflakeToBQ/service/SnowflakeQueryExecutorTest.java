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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.DDLDataDTO;
import com.google.connector.snowflakeToBQ.repository.JdbcTemplateProvider;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import java.util.*;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/** Test file for {@link SnowflakeQueryExecutor} class */
public class SnowflakeQueryExecutorTest extends AbstractTestBase {

  @Autowired SnowflakeQueryExecutor snowflakeQueryExecutor;

  @MockBean JdbcTemplateProvider jdbcTemplateRepo;

  @Test
  public void testExtractDDL() {
    String expectedDDL = "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE\")";
    DDLDataDTO ddlDataDTO = new DDLDataDTO();
    ddlDataDTO.setSourceTableName("table1");
    ddlDataDTO.setSourceDatabaseName("new_database");
    ddlDataDTO.setSourceSchemaName("test_schema");
    JdbcTemplate jdbcTemplate1 = mock(JdbcTemplate.class);
    when(jdbcTemplateRepo.getOrCreateJdbcTemplate(anyString(), anyString()))
        .thenReturn(jdbcTemplate1);
    when(jdbcTemplate1.queryForObject(any(), eq(String.class))).thenReturn(expectedDDL);
    String actualDDL = snowflakeQueryExecutor.extractDDL(ddlDataDTO);
    Assert.assertEquals(expectedDDL, actualDDL);
  }

  @Test
  public void testExtractDDLJdbcTemplateNull() {
    try {
      when(jdbcTemplateRepo.getOrCreateJdbcTemplate(anyString(), anyString())).thenReturn(null);
      DDLDataDTO ddlDataDTO = new DDLDataDTO();
      ddlDataDTO.setSourceTableName("table1");
      ddlDataDTO.setSourceDatabaseName("new_database");
      ddlDataDTO.setSourceSchemaName("test_schema");
      String actualDDL = snowflakeQueryExecutor.extractDDL(ddlDataDTO);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testGetAllTableNames() {
    List<DDLDataDTO> expectedTableNames = new ArrayList<>();

    JdbcTemplate jdbcTemplate1 = mock(JdbcTemplate.class);

    when(jdbcTemplateRepo.getOrCreateJdbcTemplate(anyString(), anyString()))
        .thenReturn(jdbcTemplate1);
    when(jdbcTemplate1.query(anyString(), any(RowMapper.class))).thenReturn(expectedTableNames);

    DDLDataDTO ddlDataDTO = new DDLDataDTO();
    ddlDataDTO.setSourceTableName("table1");
    ddlDataDTO.setSourceDatabaseName("new_database");
    ddlDataDTO.setSourceSchemaName("test_schema");
    expectedTableNames.add(ddlDataDTO);
    List<DDLDataDTO> actualTableNames = snowflakeQueryExecutor.getAllTableNames(ddlDataDTO);
    Assert.assertEquals(expectedTableNames, actualTableNames);
  }

  @Test
  public void testGetAllTableNamesJdbcTemplateNull() {
    try {
      when(jdbcTemplateRepo.getOrCreateJdbcTemplate(anyString(), anyString())).thenReturn(null);
      DDLDataDTO ddlDataDTO = new DDLDataDTO();
      ddlDataDTO.setSourceTableName("public");
      ddlDataDTO.setSourceDatabaseName("new_database");
      ddlDataDTO.setSourceSchemaName("test_schema");
      List<DDLDataDTO> actualTableNames = snowflakeQueryExecutor.getAllTableNames(ddlDataDTO);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.JDBC_EXECUTION_EXCEPTION.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.JDBC_EXECUTION_EXCEPTION.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testGetAllTableNamesJdbcTemplateNullScenario2() {
    try {
      when(jdbcTemplateRepo.getOrCreateJdbcTemplate(anyString(), anyString())).thenReturn(null);
      DDLDataDTO ddlDataDTO = new DDLDataDTO();
      ddlDataDTO.setSourceTableName("table1");
      ddlDataDTO.setSourceDatabaseName("new_database");
      ddlDataDTO.setSourceSchemaName("test_schema");
      List<DDLDataDTO> actualTableNames = snowflakeQueryExecutor.getAllTableNames(ddlDataDTO);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.JDBC_EXECUTION_EXCEPTION.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.JDBC_EXECUTION_EXCEPTION.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testAllTableDDLs() {

    List<DDLDataDTO> expectedTableNames = new ArrayList<>();

    JdbcTemplate jdbcTemplate1 = mock(JdbcTemplate.class);

    when(jdbcTemplateRepo.getOrCreateJdbcTemplate(anyString(), anyString()))
        .thenReturn(jdbcTemplate1);

    String expectedDDL1 = "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE\")";
    String expectedDDL2 = "create or replace TABLE `project`.dataset.table2 ( DATECOL DATE\")";

    when(jdbcTemplate1.queryForObject(any(), eq(String.class)))
        .thenReturn(expectedDDL1)
        .thenReturn(expectedDDL2);

    DDLDataDTO ddlDataDTO = new DDLDataDTO();
    ddlDataDTO.setSourceTableName("table1");
    ddlDataDTO.setSourceDatabaseName("new_database");
    ddlDataDTO.setSourceSchemaName("test_schema");
    expectedTableNames.add(ddlDataDTO);

    DDLDataDTO ddlDataDTO1 = new DDLDataDTO();
    ddlDataDTO1.setSourceTableName("table2");
    ddlDataDTO1.setSourceDatabaseName("new_database");
    ddlDataDTO1.setSourceSchemaName("test_schema");
    expectedTableNames.add(ddlDataDTO1);

    when(jdbcTemplate1.query(anyString(), any(RowMapper.class))).thenReturn(expectedTableNames);

    Map<String, String> actualTableNames = snowflakeQueryExecutor.getAllTableDDLs(ddlDataDTO);
    Map<String, String> expectedOutput = new HashMap<>();
    expectedOutput.put("table1", expectedDDL1);
    expectedOutput.put("table2", expectedDDL2);
    Assert.assertEquals(expectedOutput, actualTableNames);
  }

  @Test
  public void testMultipleTableDDLs() {

    String mockedDDL = "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE\")";
    String mockedDDL1 = "create or replace TABLE `project`.dataset.table2 ( DATECOL DATE\")";
    JdbcTemplate jdbcTemplate1 = mock(JdbcTemplate.class);

    when(jdbcTemplateRepo.getOrCreateJdbcTemplate(anyString(), anyString()))
        .thenReturn(jdbcTemplate1);

    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("table1", mockedDDL);
    expectedMap.put("table2", mockedDDL1);

    when(jdbcTemplate1.queryForObject(anyString(), eq(String.class)))
        .thenReturn(mockedDDL)
        .thenReturn(mockedDDL1);

    DDLDataDTO ddlDataDTO = new DDLDataDTO();
    ddlDataDTO.setSourceTableName("table1,table2");
    ddlDataDTO.setSourceDatabaseName("new_database");
    ddlDataDTO.setSourceSchemaName("test_schema");
    Map<String, String> actualTableNames = snowflakeQueryExecutor.multipleTableDDLS(ddlDataDTO);
    Assert.assertEquals(expectedMap, actualTableNames);
    Assert.assertEquals(2, actualTableNames.size());
  }

  @Test
  public void testPropertyCoverage() {

    String mockedDDL = "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE\")";

    JdbcTemplate jdbcTemplate1 = mock(JdbcTemplate.class);
    when(jdbcTemplateRepo.getOrCreateJdbcTemplate(anyString(), anyString()))
        .thenReturn(jdbcTemplate1);

    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("table1", mockedDDL);
    snowflakeQueryExecutor.setUrl("https://testing.snowflakecomputing.com");
    snowflakeQueryExecutor.setAuthenticatorType("OAuth");
    when(jdbcTemplate1.queryForObject(anyString(), eq(String.class))).thenReturn(mockedDDL);
    DDLDataDTO ddlDataDTO = new DDLDataDTO();
    ddlDataDTO.setSourceTableName("table1");
    ddlDataDTO.setSourceDatabaseName("new_database");
    ddlDataDTO.setSourceSchemaName("test_schema");
    Map<String, String> actualTableNames = snowflakeQueryExecutor.multipleTableDDLS(ddlDataDTO);
    Assert.assertEquals(expectedMap, actualTableNames);
  }
}
