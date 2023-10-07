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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.google.cloud.bigquery.*;
import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.BigQueryDetailsDataDTO;
import com.google.connector.snowflakeToBQ.service.Instancecreator.BigQueryInstanceCreator;
import com.google.connector.snowflakeToBQ.service.bigqueryjoboptions.LoadOption;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

public class BigQueryOperationsServiceTest extends AbstractTestBase {

  public static final String CREATE_TABLE_COMMAND =
      "create or replace TABLE `project`.dataset.table1 ( DATECOL DATE\")";
  @Autowired BigQueryOperationsService bigQueryOperationsService;

  @MockBean BigQueryInstanceCreator bigQueryInstanceCreator;

  @MockBean GoogleCloudStorageService googleCloudStorageService;

  @Test
  public void testLoadBigQueryJob() throws InterruptedException {

    BigQueryDetailsDataDTO bigQueryDetailsDto = new BigQueryDetailsDataDTO();
    bigQueryDetailsDto.setTableName("source_table");
    bigQueryDetailsDto.setProjectId("my_project");
    bigQueryDetailsDto.setDatasetId("my_dataset");
    bigQueryDetailsDto.setBucketName("bucketname");
    bigQueryDetailsDto.setGcsDDLFilePath("gs://ddlpath");
    bigQueryDetailsDto.setSnowflakeDataUnloadGCSPath("gs://snowflake_unload_path");
    bigQueryDetailsDto.setBqLoadFileFormat(LoadOption.CSV.getLoadOption());

    BigQuery bigqueryMock = mock(BigQuery.class);
    Table tableMock = mock(Table.class);
    TableDefinition tableDefinitionMock = mock(TableDefinition.class);

    when(bigQueryInstanceCreator.getBigQueryClient()).thenReturn(bigqueryMock);
    when(bigqueryMock.getTable(any(TableId.class))).thenReturn(tableMock);
    when(tableMock.getDefinition()).thenReturn(tableDefinitionMock);

    Schema schema =
        Schema.of(
            Field.of("name", StandardSQLTypeName.STRING),
            Field.of("age", StandardSQLTypeName.INT64));
    when(tableDefinitionMock.getSchema()).thenReturn(schema);
    Job mockJob = mock(Job.class);
    JobStatus mockJobStatus = mock(JobStatus.class);

    when(bigqueryMock.create(any(JobInfo.class))).thenReturn(mockJob);
    // Set a value in the mock job
    when(mockJob.waitFor()).thenReturn(mockJob);
    when(mockJob.getStatus()).thenReturn(mockJobStatus);
    when(mockJobStatus.getError()).thenReturn(null);
    boolean jobStatus = bigQueryOperationsService.loadBigQueryJob(bigQueryDetailsDto);
    Assert.assertTrue(jobStatus);
  }

  @Test
  public void testLoadBigQueryJobNegative() throws InterruptedException {

    BigQueryDetailsDataDTO bigQueryDetailsDto = new BigQueryDetailsDataDTO();
    bigQueryDetailsDto.setTableName("source_table");
    bigQueryDetailsDto.setProjectId("my_project");
    bigQueryDetailsDto.setDatasetId("my_dataset");
    bigQueryDetailsDto.setBucketName("bucketname");
    bigQueryDetailsDto.setGcsDDLFilePath("gs://ddlpath");
    bigQueryDetailsDto.setSnowflakeDataUnloadGCSPath("gs://snowflake_unload_path");
    bigQueryDetailsDto.setBqLoadFileFormat(LoadOption.CSV.name());

    BigQuery bigqueryMock = mock(BigQuery.class);
    Table tableMock = mock(Table.class);
    TableDefinition tableDefinitionMock = mock(TableDefinition.class);

    when(bigQueryInstanceCreator.getBigQueryClient()).thenReturn(bigqueryMock);
    when(bigqueryMock.getTable(any(TableId.class))).thenReturn(tableMock);
    when(tableMock.getDefinition()).thenReturn(tableDefinitionMock);

    Schema schema =
        Schema.of(
            Field.of("name", StandardSQLTypeName.STRING),
            Field.of("age", StandardSQLTypeName.INT64));
    when(tableDefinitionMock.getSchema()).thenReturn(schema);
    Job mockJob = mock(Job.class);
    JobStatus mockJobStatus = mock(JobStatus.class);

    when(bigqueryMock.create(any(JobInfo.class))).thenReturn(mockJob);
    // Set a value in the mock job
    when(mockJob.waitFor()).thenReturn(mockJob);
    when(mockJob.getStatus()).thenReturn(mockJobStatus);
    when(mockJobStatus.getError())
        .thenReturn(
            new BigQueryError("Test Error", "in test", "This is mocked error from test case"));
    boolean jobStatus = bigQueryOperationsService.loadBigQueryJob(bigQueryDetailsDto);
    Assert.assertFalse(jobStatus);
  }

  @Test
  public void testLoadBigQueryJobNegativeLoadJobNull() throws InterruptedException {

    BigQueryDetailsDataDTO bigQueryDetailsDto = new BigQueryDetailsDataDTO();
    bigQueryDetailsDto.setTableName("source_table");
    bigQueryDetailsDto.setProjectId("my_project");
    bigQueryDetailsDto.setDatasetId("my_dataset");
    bigQueryDetailsDto.setBucketName("bucketname");
    bigQueryDetailsDto.setGcsDDLFilePath("gs://ddlpath");
    bigQueryDetailsDto.setSnowflakeDataUnloadGCSPath("gs://snowflake_unload_path");
    bigQueryDetailsDto.setBqLoadFileFormat(LoadOption.CSV.name());

    BigQuery bigqueryMock = mock(BigQuery.class);
    Table tableMock = mock(Table.class);
    TableDefinition tableDefinitionMock = mock(TableDefinition.class);

    when(bigQueryInstanceCreator.getBigQueryClient()).thenReturn(bigqueryMock);
    when(bigqueryMock.getTable(any(TableId.class))).thenReturn(tableMock);
    when(tableMock.getDefinition()).thenReturn(tableDefinitionMock);

    Schema schema =
        Schema.of(
            Field.of("name", StandardSQLTypeName.STRING),
            Field.of("age", StandardSQLTypeName.INT64));
    when(tableDefinitionMock.getSchema()).thenReturn(schema);
    Job mockJob = mock(Job.class);

    when(bigqueryMock.create(any(JobInfo.class))).thenReturn(mockJob);
    // Set a value in the mock job
    when(mockJob.waitFor()).thenReturn(null);

    boolean jobStatus = bigQueryOperationsService.loadBigQueryJob(bigQueryDetailsDto);
    Assert.assertFalse(jobStatus);
  }

  @Test
  public void testLoadBigQueryJobNegativeThrowException() throws InterruptedException {

    BigQueryDetailsDataDTO bigQueryDetailsDto = new BigQueryDetailsDataDTO();
    bigQueryDetailsDto.setTableName("source_table");
    bigQueryDetailsDto.setProjectId("my_project");
    bigQueryDetailsDto.setDatasetId("my_dataset");
    bigQueryDetailsDto.setBucketName("bucketname");
    bigQueryDetailsDto.setGcsDDLFilePath("gs://ddlpath");
    bigQueryDetailsDto.setSnowflakeDataUnloadGCSPath("gs://snowflake_unload_path");
    bigQueryDetailsDto.setBqLoadFileFormat(LoadOption.CSV.name());

    BigQuery bigqueryMock = mock(BigQuery.class);
    Table tableMock = mock(Table.class);
    TableDefinition tableDefinitionMock = mock(TableDefinition.class);

    when(bigQueryInstanceCreator.getBigQueryClient()).thenReturn(bigqueryMock);
    when(bigqueryMock.getTable(any(TableId.class))).thenReturn(tableMock);
    when(tableMock.getDefinition()).thenReturn(tableDefinitionMock);

    Schema schema =
        Schema.of(
            Field.of("name", StandardSQLTypeName.STRING),
            Field.of("age", StandardSQLTypeName.INT64));
    when(tableDefinitionMock.getSchema()).thenReturn(schema);
    Job mockJob = mock(Job.class);

    doThrow(RuntimeException.class).when(bigqueryMock).create(any(JobInfo.class));
    when(mockJob.waitFor()).thenReturn(null);
    try {
      boolean jobStatus = bigQueryOperationsService.loadBigQueryJob(bigQueryDetailsDto);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.BQ_QUERY_JOB_EXECUTION_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.BQ_QUERY_JOB_EXECUTION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testCreateTable() throws InterruptedException {

    BigQuery bigqueryMock = mock(BigQuery.class);

    when(bigQueryInstanceCreator.getBigQueryClient()).thenReturn(bigqueryMock);
    when(bigqueryMock.getTable(any(TableId.class))).thenReturn(null);
    when(googleCloudStorageService.getContentFromGCSFile(anyString(), anyString()))
        .thenReturn(CREATE_TABLE_COMMAND);

    Job mockJob = mock(Job.class);
    JobStatus mockJobStatus = mock(JobStatus.class);

    // Need this setting for private method (queryJob())
    when(bigqueryMock.create(any(JobInfo.class))).thenReturn(mockJob);
    // Set a value in the mock job
    when(mockJob.waitFor()).thenReturn(mockJob);
    when(mockJob.getStatus()).thenReturn(mockJobStatus);
    when(mockJobStatus.getError()).thenReturn(null);
    when(mockJob.isDone()).thenReturn(true);

    boolean jobStatus = bigQueryOperationsService.createTableUsingDDL(CREATE_TABLE_COMMAND, "us");
    Assert.assertTrue(jobStatus);
  }

  @Test
  public void testCreateTableJobFail() throws InterruptedException {

    BigQuery bigqueryMock = mock(BigQuery.class);

    when(bigQueryInstanceCreator.getBigQueryClient()).thenReturn(bigqueryMock);
    when(bigqueryMock.getTable(any(TableId.class))).thenReturn(null);
    when(googleCloudStorageService.getContentFromGCSFile(anyString(), anyString()))
        .thenReturn(CREATE_TABLE_COMMAND);

    Job mockJob = mock(Job.class);
    JobStatus mockJobStatus = mock(JobStatus.class);

    // Need this setting for private method (queryJob())
    when(bigqueryMock.create(any(JobInfo.class))).thenReturn(mockJob);
    // Set a value in the mock job
    when(mockJob.waitFor()).thenReturn(mockJob);
    when(mockJob.getStatus()).thenReturn(mockJobStatus);
    when(mockJobStatus.getError()).thenReturn(null);
    when(mockJob.isDone()).thenReturn(false);
    try {
      boolean jobStatus = bigQueryOperationsService.createTableUsingDDL(CREATE_TABLE_COMMAND, "us");
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.TABLE_CREATION_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.TABLE_CREATION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testCreateTableJobFailWithBigQueryError() throws InterruptedException {
    BigQuery bigqueryMock = mock(BigQuery.class);

    when(bigQueryInstanceCreator.getBigQueryClient()).thenReturn(bigqueryMock);
    when(bigqueryMock.getTable(any(TableId.class))).thenReturn(null);
    when(googleCloudStorageService.getContentFromGCSFile(anyString(), anyString()))
        .thenReturn(CREATE_TABLE_COMMAND);

    Job mockJob = mock(Job.class);
    JobStatus mockJobStatus = mock(JobStatus.class);

    // Need this setting for private method (queryJob())
    when(bigqueryMock.create(any(JobInfo.class))).thenReturn(mockJob);
    // Set a value in the mock job
    when(mockJob.waitFor()).thenReturn(mockJob);
    when(mockJob.getStatus()).thenReturn(mockJobStatus);
    // Setting the scenario where the error is return from BigQuery Job Execution
    when(mockJobStatus.getError())
        .thenReturn(
            new BigQueryError("Test Error", "in test", "This is mocked error from test case"));
    when(mockJob.isDone()).thenReturn(false);
    try {
      boolean jobStatus = bigQueryOperationsService.createTableUsingDDL(CREATE_TABLE_COMMAND, "");
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.TABLE_CREATION_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.TABLE_CREATION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testCreateTableJobFailWithException() {

    BigQuery bigqueryMock = mock(BigQuery.class);

    when(bigQueryInstanceCreator.getBigQueryClient()).thenReturn(bigqueryMock);
    when(bigqueryMock.getTable(any(TableId.class))).thenReturn(null);
    when(googleCloudStorageService.getContentFromGCSFile(anyString(), anyString()))
        .thenReturn(CREATE_TABLE_COMMAND);

    Job mockJob = mock(Job.class);

    // Need this setting for private method (queryJob())
    when(bigqueryMock.create(any(JobInfo.class))).thenReturn(mockJob);
    // Set a value in the mock job
    doThrow(RuntimeException.class).when(bigqueryMock).create(any(JobInfo.class));
    try {
      boolean jobStatus = bigQueryOperationsService.createTableUsingDDL(CREATE_TABLE_COMMAND, null);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.BQ_QUERY_JOB_EXECUTION_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.BQ_QUERY_JOB_EXECUTION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testLoadCreateTableBlankDDL() {

    try {
      boolean jobStatus = bigQueryOperationsService.createTableUsingDDL("", "");
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(ErrorCode.TABLE_CREATION_ERROR.getMessage(), e.getMessage());
      Assert.assertEquals(ErrorCode.TABLE_CREATION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }
}
