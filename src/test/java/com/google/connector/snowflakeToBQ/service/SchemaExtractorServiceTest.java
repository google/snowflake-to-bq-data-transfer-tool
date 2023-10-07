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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.DDLDataDTO;
import com.google.connector.snowflakeToBQ.repository.SnowflakesJdbcDataRepository;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class SchemaExtractorServiceTest {

  private SchemaExtractorService schemaExtractorService;
  @MockBean SnowflakesJdbcDataRepository snowflakesJdbcDataRepository;

  @Before
  public void setup() {
    schemaExtractorService = new SchemaExtractorService(snowflakesJdbcDataRepository);
  }

  @Test()
  public void testIsSchemaTrue() {
    Map<String, String> ddlMapForSchema = new HashMap<>();
    ddlMapForSchema.put(
        "table1", "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE");
    ddlMapForSchema.put(
        "table2",
        "create or replace TABLE `project`.dataset.table2 ( DATECOL DATE,DATETIMECOL"
            + " DATETIME,TIMESTAMPCOL  TIMESTAMP");
    when(snowflakesJdbcDataRepository.getAllTableDDLs(anyString())).thenReturn(ddlMapForSchema);
    DDLDataDTO dto = new DDLDataDTO();
    dto.setSchema(true);
    dto.setSourceSchemaName("public");
    Map<String, String> actualMap = schemaExtractorService.getDDLs(dto);
    Assert.assertEquals(ddlMapForSchema, actualMap);
  }

  @Test
  public void testIsSchemaFalseTableNameNotGiven() {
    Map<String, String> ddlMapForSchema = new HashMap<>();
    ddlMapForSchema.put(
        "table1", "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE");
    ddlMapForSchema.put(
        "table2",
        "create or replace TABLE `project`.dataset.table2 ( DATECOL DATE,DATETIMECOL"
            + " DATETIME,TIMESTAMPCOL  TIMESTAMP");
    when(snowflakesJdbcDataRepository.multipleTableDDLS(any(String[].class)))
        .thenReturn(ddlMapForSchema);
    DDLDataDTO dto = new DDLDataDTO();
    dto.setSchema(false);
    dto.setSourceSchemaName("public");
    try {
      Map<String, String> actualMap = schemaExtractorService.getDDLs(dto);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(e.getMessage(), ErrorCode.TABLE_NAME_NOT_PRESENT_IN_REQUEST.getMessage());
      Assert.assertEquals(
          e.getErrorCode(), ErrorCode.TABLE_NAME_NOT_PRESENT_IN_REQUEST.getErrorCode());
    }
  }

  @Test
  public void testIsSchemaFalseTableNameGiven() {
    Map<String, String> ddlMapForSchema = new HashMap<>();
    ddlMapForSchema.put(
        "table1", "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE");
    ddlMapForSchema.put(
        "table2",
        "create or replace TABLE `project`.dataset.table2 ( DATECOL DATE,DATETIMECOL"
            + " DATETIME,TIMESTAMPCOL  TIMESTAMP");
    when(snowflakesJdbcDataRepository.multipleTableDDLS(any(String[].class)))
        .thenReturn(ddlMapForSchema);
    DDLDataDTO dto = new DDLDataDTO();
    dto.setSchema(false);
    dto.setSourceTableName("table1");
    Map<String, String> actualMap = schemaExtractorService.getDDLs(dto);
    Assert.assertEquals(ddlMapForSchema.get("table1"), actualMap.get("table1"));
  }
}
