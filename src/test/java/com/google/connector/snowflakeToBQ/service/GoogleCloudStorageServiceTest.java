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

import static org.mockito.Mockito.*;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.model.datadto.GCSDetailsDataDTO;
import com.google.connector.snowflakeToBQ.repository.ApplicationConfigDataRepository;
import com.google.connector.snowflakeToBQ.service.Instancecreator.StorageInstanceCreator;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.event.annotation.BeforeTestMethod;

public class GoogleCloudStorageServiceTest extends AbstractTestBase {

  @Autowired GoogleCloudStorageService googleCloudStorageService;

  @MockBean StorageInstanceCreator storageInstanceCreator;
  @Autowired ApplicationConfigDataService applicationConfigDataService;
  @Autowired ApplicationConfigDataRepository applicationConfigDataRepository;

  @BeforeTestMethod
  public void cleanup() {
    applicationConfigDataRepository.deleteAll();
  }

  @Test
  @Ignore
  public void testWriteToGCS() {
    String ddl = "create or replace TABLE table1 ( DATECOL DATE\")";
    Storage storageMock = mock(Storage.class);
    Blob blobMock = mock(Blob.class);

    when(storageInstanceCreator.getStorageClient()).thenReturn(storageMock);
    String updatedDDl =
        ddl.replace(
            "table1", String.format("%s.%s.%s", "source_database", "source_schema", "table1"));
    // In below mock we are matching  argument that is equal to the given value, hence updating the
    // table name with database and schema as that is happening in code before below mocks gets
    // called.
    when(storageMock.create(any(BlobInfo.class), eq(updatedDDl.getBytes(StandardCharsets.UTF_8))))
        .thenReturn(blobMock);
    when(blobMock.getBucket()).thenReturn("test_bucket");
    when(blobMock.getName()).thenReturn("test_name");

    Map<String, String> ddlMap = new HashMap<>();
    ddlMap.put("table1", ddl);

    GCSDetailsDataDTO gcsDetailsDataDTO = new GCSDetailsDataDTO();
    gcsDetailsDataDTO.setSourceTableName("table1");
    gcsDetailsDataDTO.setSourceSchemaName("source_schema");
    gcsDetailsDataDTO.setSourceDatabaseName("source_database");
    gcsDetailsDataDTO.setGcsBucketForDDLs("gs://testing");
    gcsDetailsDataDTO.setSourceDatabaseName("source_database");

    List<GCSDetailsDataDTO> gcsDetailsDataDTOS =
        googleCloudStorageService.writeToGCS(ddlMap, gcsDetailsDataDTO);
    Assert.assertEquals(1, gcsDetailsDataDTOS.size());

    Assert.assertEquals(
        gcsDetailsDataDTO.getSourceTableName(), gcsDetailsDataDTOS.get(0).getSourceTableName());
    // here the test_bucket and test_name are given in above ,mock method hence its verified in
    // below call.
    Assert.assertEquals(
        "gs://test_bucket/test_name", gcsDetailsDataDTOS.get(0).getSnowflakeDDLsPath());
    Assert.assertTrue(gcsDetailsDataDTOS.get(0).isSourceDDLCopied());
  }

  @Test
  @Ignore
  public void testMoveFolder() {
    String ddl = "create or replace TABLE table1 ( DATECOL DATE\")";
    Storage storageMock = mock(Storage.class);
    Blob blobMock = mock(Blob.class);

    when(storageInstanceCreator.getStorageClient()).thenReturn(storageMock);
    String updatedDDl =
        ddl.replace(
            "table1", String.format("%s.%s.%s", "source_database", "source_schema", "table1"));
    // In below mock we are matching  argument that is equal to the given value, hence updating the
    // table name with database and schema as that is happening in code before below mocks gets
    // called.
    when(storageMock.create(any(BlobInfo.class), eq(updatedDDl.getBytes(StandardCharsets.UTF_8))))
        .thenReturn(blobMock);
    when(blobMock.getBucket()).thenReturn("test_bucket");
    when(blobMock.getName()).thenReturn("test_name");

    Map<String, String> ddlMap = new HashMap<>();
    ddlMap.put("table1", ddl);
    GCSDetailsDataDTO gcsDetailsDataDTO = new GCSDetailsDataDTO();
    gcsDetailsDataDTO.setSourceTableName("table1");
    gcsDetailsDataDTO.setSourceSchemaName("source_schema");
    gcsDetailsDataDTO.setSourceDatabaseName("source_database");
    gcsDetailsDataDTO.setGcsBucketForDDLs("gs://testing");
    gcsDetailsDataDTO.setSourceDatabaseName("source_database");

    List<GCSDetailsDataDTO> gcsDetailsDataDTOS =
        googleCloudStorageService.writeToGCS(ddlMap, gcsDetailsDataDTO);
    Assert.assertEquals(1, gcsDetailsDataDTOS.size());
    Assert.assertEquals(
        gcsDetailsDataDTO.getSourceTableName(), gcsDetailsDataDTOS.get(0).getSourceTableName());
    // here the test_bucket and test_name are given in above ,mock method hence its verified in
    // below call.
    Assert.assertEquals(
        "gs://test_bucket/test_name", gcsDetailsDataDTOS.get(0).getSnowflakeDDLsPath());
    Assert.assertTrue(gcsDetailsDataDTOS.get(0).isSourceDDLCopied());
  }

  /**
   * Tests the `replaceSnowflakeTableNameWithBQ` private method in `GoogleCloudStorageService`. This
   * test covers various scenarios including: - Case insensitivity of the "TABLE" keyword - Varying
   * whitespace around the "TABLE" keyword - Multiple table occurrences in the DDL (replace all
   * where table keyword is present) - Edge cases like no "TABLE" keyword, empty DDL, table name at
   * start/end
   *
   * @throws NoSuchMethodException If the method is not found
   * @throws InvocationTargetException If the method invocation throws an exception
   * @throws IllegalAccessException If the method cannot be accessed
   */
  @Test
  public void testReplaceSnowflakeTableNameWithBQ()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method privateMethod =
        GoogleCloudStorageService.class.getDeclaredMethod(
            "replaceSnowflakeTableNameWithBQ", String.class, String.class);
    privateMethod.setAccessible(true);

    // Table name present in column name, only table name should get replaced
    testAndVerify(
        privateMethod,
        "create or replace TABLE INVENTORY (\n"
            + "            INVENTORY_ID NUMBER(38,0),\n"
            + "            PRODUCT_ID NUMBER(38,0),\n"
            + "            QUANTITY NUMBER(38,0)\n"
            + "    );",
        "my_bq_table",
        "create or replace TABLE my_bq_table (\n"
            + "            INVENTORY_ID NUMBER(38,0),\n"
            + "            PRODUCT_ID NUMBER(38,0),\n"
            + "            QUANTITY NUMBER(38,0)\n"
            + "    );");
    // Case Insensitivity
    testAndVerify(
        privateMethod,
        "create or replace TABLE My_Snowflake_Table (id INT, name VARCHAR(255))",
        "my_bq_table",
        "create or replace TABLE my_bq_table (id INT, name VARCHAR(255))");
    testAndVerify(
        privateMethod,
        "CREATE or replace table another_table (...)",
        "bq_table",
        "CREATE or replace table bq_table (...)");

    // Varying Whitespace
    testAndVerify(
        privateMethod,
        "CREATE or REPLACE     TABLE   table_with_spaces   (...)",
        "no_spaces",
        "CREATE or REPLACE     TABLE no_spaces   (...)");

    // Multiple Tables (Replaces all occurrence)
    testAndVerify(
        privateMethod,
        "CREATE TABLE table1 (...) JOIN TABLE table2 ON ...",
        "new_table1",
        "CREATE TABLE new_table1 (...) JOIN TABLE new_table1 ON ...");

    // Edge Cases
    // No TABLE Keyword no replacement
    testAndVerify(privateMethod, "SELECT * FROM some_view", "anything", "SELECT * FROM some_view");

    // Empty DDL
    testAndVerify(privateMethod, "", "anything", "");

    // Table Name at Start/End, still replaced
    testAndVerify(privateMethod, "TABLE my_table (...)", "start_table", "TABLE start_table (...)");
    testAndVerify(privateMethod, "(...) FROM old_table", "end_table", "(...) FROM old_table");

    // Table Name with Schema, schema will get replaced
    testAndVerify(
        privateMethod,
        "CREATE TABLE my_schema.my_table (...)",
        "new_table",
        "CREATE TABLE new_table.my_table (...)");
  }

  /** Helper method * @throws IllegalAccessException */
  private void testAndVerify(Method method, String ddl, String newTableName, String expected)
      throws InvocationTargetException, IllegalAccessException {
    String result = (String) method.invoke(googleCloudStorageService, ddl, newTableName);
    Assert.assertEquals(expected, result);
  }
}
