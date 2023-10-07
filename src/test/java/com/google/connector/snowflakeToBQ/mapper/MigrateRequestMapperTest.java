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

package com.google.connector.snowflakeToBQ.mapper;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.entity.ApplicationConfigData;
import com.google.connector.snowflakeToBQ.model.datadto.BigQueryDetailsDataDTO;
import com.google.connector.snowflakeToBQ.model.datadto.DDLDataDTO;
import com.google.connector.snowflakeToBQ.model.datadto.GCSDetailsDataDTO;
import com.google.connector.snowflakeToBQ.model.datadto.TranslateDDLDataDTO;
import com.google.connector.snowflakeToBQ.model.request.SFDataMigrationRequestDTO;
import org.junit.Assert;
import org.junit.Test;

public class MigrateRequestMapperTest extends AbstractTestBase {

  @Test
  public void testMigrateRequestMapper() {
    SFDataMigrationRequestDTO sfDataMigrationRequestDTO = new SFDataMigrationRequestDTO();

    sfDataMigrationRequestDTO.setSourceDatabaseName("SourceDB");
    sfDataMigrationRequestDTO.setSourceSchemaName("SourceSchema");
    sfDataMigrationRequestDTO.setTargetDatabaseName("TargetDB");
    sfDataMigrationRequestDTO.setTargetSchemaName("TargetSchema");
    sfDataMigrationRequestDTO.setSourceTableName("SourceTable");
    sfDataMigrationRequestDTO.setGcsBucketForDDLs("GcsBucketForDDLs");
    sfDataMigrationRequestDTO.setGcsBucketForTranslation("GcsBucketForTranslation");
    sfDataMigrationRequestDTO.setSchema(true);
    sfDataMigrationRequestDTO.setLocation("Location");
    sfDataMigrationRequestDTO.setSnowflakeStageLocation("SnowflakeStageLocation");
    // This line added to just cover the migrate request without adding a new class for it.
    sfDataMigrationRequestDTO.toString();

    DDLDataDTO ddlDataDTO =
        MigrateRequestMapper.getDDLDataDTOFromSFDataMigrationRequestDTO(sfDataMigrationRequestDTO);
    Assert.assertEquals("SourceDB", ddlDataDTO.getSourceDatabaseName());
    Assert.assertEquals("SourceSchema", ddlDataDTO.getSourceSchemaName());
    Assert.assertEquals("TargetDB", ddlDataDTO.getTargetDatabaseName());
    Assert.assertEquals("TargetSchema", ddlDataDTO.getTargetSchemaName());
    Assert.assertEquals("SourceTable", ddlDataDTO.getSourceTableName());

    GCSDetailsDataDTO gcsDetailsDataDTO =
        MigrateRequestMapper.getGCSDetailsDataDTOFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);

    Assert.assertEquals("GcsBucketForDDLs", gcsDetailsDataDTO.getGcsBucketForDDLs());
    Assert.assertEquals("SourceDB", gcsDetailsDataDTO.getSourceDatabaseName());

    TranslateDDLDataDTO translateDDLDataDTO =
        MigrateRequestMapper.getTranslateDDLDataDTOFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);
    Assert.assertEquals(
        "GcsBucketForTranslation", translateDDLDataDTO.getGcsBucketForTranslation());
    Assert.assertTrue(ddlDataDTO.isSchema());
    Assert.assertEquals("Location", sfDataMigrationRequestDTO.getLocation());
    Assert.assertEquals(
        "SnowflakeStageLocation", sfDataMigrationRequestDTO.getSnowflakeStageLocation());
  }

  @Test
  public void testMigrateRequestToApplicationDataEntity() {
    SFDataMigrationRequestDTO sfDataMigrationRequestDTO = new SFDataMigrationRequestDTO();
    sfDataMigrationRequestDTO.setSourceDatabaseName("SourceDB");
    sfDataMigrationRequestDTO.setSourceSchemaName("SourceSchema");
    sfDataMigrationRequestDTO.setSourceTableName("SourceTable");
    sfDataMigrationRequestDTO.setTargetDatabaseName("TargetDB");
    sfDataMigrationRequestDTO.setTargetSchemaName("TargetSchema");
    sfDataMigrationRequestDTO.setGcsBucketForDDLs("GcsBucketForDDLs");
    sfDataMigrationRequestDTO.setGcsBucketForTranslation("GcsBucketForTranslation");
    sfDataMigrationRequestDTO.setSchema(true);
    sfDataMigrationRequestDTO.setLocation("Location");
    sfDataMigrationRequestDTO.setSnowflakeStageLocation("SnowflakeStageLocation");

    // Convert MigrateRequestDto to ApplicationConfigData
    ApplicationConfigData applicationConfigData =
        MigrateRequestMapper.getApplicationConfigEntityFromSFDataMigrationRequestDTO(
            sfDataMigrationRequestDTO);

    // Assert the values using getter methods
    Assert.assertEquals("SourceDB", applicationConfigData.getSourceDatabaseName());
    Assert.assertEquals("SourceSchema", applicationConfigData.getSourceSchemaName());
    Assert.assertEquals("SourceTable", applicationConfigData.getSourceTableName());
    Assert.assertEquals("TargetDB", applicationConfigData.getTargetDatabaseName());
    Assert.assertEquals("TargetSchema", applicationConfigData.getTargetSchemaName());
    Assert.assertEquals("SourceTable", applicationConfigData.getTargetTableName());
    Assert.assertEquals("GcsBucketForDDLs", applicationConfigData.getGcsBucketForDDLs());
    Assert.assertEquals(
        "GcsBucketForTranslation", applicationConfigData.getGcsBucketForTranslation());
    Assert.assertTrue(applicationConfigData.isSchema());
    Assert.assertEquals("Location", applicationConfigData.getLocation());
    Assert.assertEquals(
        "SnowflakeStageLocation", applicationConfigData.getSnowflakeStageLocation());
  }

  @Test
  public void testMigrateRequestToDDLDto() {
    // Create a MigrateRequestDto with test values
    SFDataMigrationRequestDTO sfDataMigrationRequestDTO = new SFDataMigrationRequestDTO();
    sfDataMigrationRequestDTO.setSourceDatabaseName("SourceDB");
    sfDataMigrationRequestDTO.setSourceSchemaName("SourceSchema");
    sfDataMigrationRequestDTO.setSourceTableName("SourceTable");
    sfDataMigrationRequestDTO.setSchema(true);

    // Convert MigrateRequestDto to DDLDto
    DDLDataDTO ddlDto =
        MigrateRequestMapper.getDDLDataDTOFromSFDataMigrationRequestDTO(sfDataMigrationRequestDTO);

    // Assert the values using getter methods
    Assert.assertEquals("SourceDB", ddlDto.getSourceDatabaseName());
    Assert.assertEquals("SourceSchema", ddlDto.getSourceSchemaName());
    Assert.assertEquals("SourceTable", ddlDto.getSourceTableName());
    Assert.assertTrue(ddlDto.isSchema());
  }

  @Test
  public void testMigrateRequestToBigQueryDetailDto() {
    // Create an ApplicationConfigData object with test values
    ApplicationConfigData applicationConfigData = new ApplicationConfigData();
    applicationConfigData.setId(1L);
    applicationConfigData.setTargetDatabaseName("TargetDB");
    applicationConfigData.setTargetSchemaName("TargetSchema");
    applicationConfigData.setTargetTableName("TargetTable");
    applicationConfigData.setGcsBucketForTranslation("GcsBucketForTranslation");
    applicationConfigData.setTranslatedDDLGCSPath("TranslatedDDLGCSPath");
    applicationConfigData.setSnowflakeStageLocation("SnowflakeStageLocation");

    // Convert ApplicationConfigData to BigQueryDetailsDto
    BigQueryDetailsDataDTO bigQueryDetailsDto =
        MigrateRequestMapper.migrateRequestToBigQueryDetailDataDto(applicationConfigData);

    // Assert the values using getter methods
    Assert.assertEquals("TargetDB", bigQueryDetailsDto.getProjectId());
    Assert.assertEquals("TargetSchema", bigQueryDetailsDto.getDatasetId());
    Assert.assertEquals("TargetTable", bigQueryDetailsDto.getTableName());
    Assert.assertEquals("GcsBucketForTranslation", bigQueryDetailsDto.getBucketName());
    Assert.assertEquals("TranslatedDDLGCSPath", bigQueryDetailsDto.getGcsDDLFilePath());
    Assert.assertEquals(
        "SnowflakeStageLocation", bigQueryDetailsDto.getSnowflakeDataUnloadGCSPath());
  }
}
