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

package com.google.connector.snowflakeToBQ.mapper;

import com.google.connector.snowflakeToBQ.entity.ApplicationConfigData;
import com.google.connector.snowflakeToBQ.model.datadto.*;
import com.google.connector.snowflakeToBQ.model.request.SFDataMigrationRequestDTO;
import com.google.connector.snowflakeToBQ.model.request.SFExtractAndTranslateDDLRequestDTO;
import com.google.connector.snowflakeToBQ.model.request.SnowflakeUnloadToGCSRequestDTO;
import com.google.connector.snowflakeToBQ.model.response.SFDataMigrationResponse;
import org.slf4j.MDC;

/** Class which provides methods to map request DTOs with data DTOs */
public interface MigrateRequestMapper {

  /**
   * Method to convert {@link ApplicationConfigData} values from the {@link
   * SFDataMigrationRequestDTO}.
   */
  static ApplicationConfigData getApplicationConfigEntityFromSFDataMigrationRequestDTO(
      SFDataMigrationRequestDTO sfDataMigrationRequestDTO) {
    ApplicationConfigData applicationConfigData = new ApplicationConfigData();

    applicationConfigData.setSourceDatabaseName(sfDataMigrationRequestDTO.getSourceDatabaseName());
    applicationConfigData.setSourceSchemaName(sfDataMigrationRequestDTO.getSourceSchemaName());
    applicationConfigData.setSourceTableName(sfDataMigrationRequestDTO.getSourceTableName());
    applicationConfigData.setTargetDatabaseName(sfDataMigrationRequestDTO.getTargetDatabaseName());
    applicationConfigData.setTargetSchemaName(sfDataMigrationRequestDTO.getTargetSchemaName());
    applicationConfigData.setTargetTableName(sfDataMigrationRequestDTO.getSourceTableName());
    applicationConfigData.setWarehouse(sfDataMigrationRequestDTO.getWarehouse());
    applicationConfigData.setGcsBucketForDDLs(sfDataMigrationRequestDTO.getGcsBucketForDDLs());
    applicationConfigData.setGcsBucketForTranslation(
        sfDataMigrationRequestDTO.getGcsBucketForTranslation());
    applicationConfigData.setSchema(sfDataMigrationRequestDTO.isSchema());
    applicationConfigData.setLocation(sfDataMigrationRequestDTO.getLocation());
    applicationConfigData.setSnowflakeStageLocation(
        sfDataMigrationRequestDTO.getSnowflakeStageLocation());
    applicationConfigData.setBQTableCreated(sfDataMigrationRequestDTO.isBqTableExists());
    applicationConfigData.setBqLoadFileFormat(sfDataMigrationRequestDTO.getBqLoadFileFormat());
    applicationConfigData.setSnowflakeFileFormatValue(
        sfDataMigrationRequestDTO.getSnowflakeFileFormatValue());
    applicationConfigData.setRequestLogId(MDC.get("requestLogId"));

    return applicationConfigData;
  }

  /**
   * Method to extract {@link DDLDataDTO} values from the {@link SFDataMigrationRequestDTO}. This
   * request comes as a part of rest API which performs all the operations(extract ddl, translate
   * ddl, unload data from Sf and load BQ).
   */
  static DDLDataDTO getDDLDataDTOFromSFDataMigrationRequestDTO(
      SFDataMigrationRequestDTO sfDataMigrationRequestDTO) {
    return getDdlDataDTO(
        sfDataMigrationRequestDTO.getSourceDatabaseName(),
        sfDataMigrationRequestDTO.getSourceSchemaName(),
        sfDataMigrationRequestDTO.getTargetDatabaseName(),
        sfDataMigrationRequestDTO.getTargetSchemaName(),
        sfDataMigrationRequestDTO.getSourceTableName(),
        sfDataMigrationRequestDTO.isSchema());
  }

  /**
   * Method to extract {@link DDLDataDTO} values from the {@link SFExtractAndTranslateDDLRequestDTO}
   * This request comes as a part of different rest API which only unload data from Snowflake to
   * GCS.
   */
  static DDLDataDTO getDDLDataDTOFromExtractDDLRequestDTO(
      SFExtractAndTranslateDDLRequestDTO extractDDLRequestDTO) {
    return getDdlDataDTO(
        extractDDLRequestDTO.getSourceDatabaseName(),
        extractDDLRequestDTO.getSourceSchemaName(),
        extractDDLRequestDTO.getTargetDatabaseName(),
        extractDDLRequestDTO.getTargetSchemaName(),
        extractDDLRequestDTO.getSourceTableName(),
        extractDDLRequestDTO.isSchema());
  }

  /**
   * Method to extract {@link GCSDetailsDataDTO} values from the {@link SFDataMigrationRequestDTO}
   * request comes as a part of rest API which performs all the operations(extract ddl, translate
   * ddl, unload data from Sf and load BQ).
   */
  static GCSDetailsDataDTO getGCSDetailsDataDTOFromSFDataMigrationRequestDTO(
      SFDataMigrationRequestDTO sfDataMigrationRequestDTO) {
    return getGcsDetailsDataDTO(
        sfDataMigrationRequestDTO.getSourceDatabaseName(),
        sfDataMigrationRequestDTO.getSourceSchemaName(),
        sfDataMigrationRequestDTO.getSourceTableName(),
        sfDataMigrationRequestDTO.getTargetDatabaseName(),
        sfDataMigrationRequestDTO.getTargetSchemaName(),
        sfDataMigrationRequestDTO.getGcsBucketForDDLs());
  }

  /**
   * Method to extract {@link GCSDetailsDataDTO} values from the {@link
   * SFExtractAndTranslateDDLRequestDTO} This request comes as a part of different rest API which
   * only extract DDL and translate them.
   */
  static GCSDetailsDataDTO getGCSDetailsDataDTOFromExtractDDLRequestDTO(
      SFExtractAndTranslateDDLRequestDTO taskRequestDTO) {
    return getGcsDetailsDataDTO(
        taskRequestDTO.getSourceDatabaseName(),
        taskRequestDTO.getSourceSchemaName(),
        taskRequestDTO.getSourceTableName(),
        taskRequestDTO.getTargetDatabaseName(),
        taskRequestDTO.getTargetSchemaName(),
        taskRequestDTO.getGcsBucketForDDLs());
  }

  /**
   * Method to extract {@link TranslateDDLDataDTO} values from the {@link SFDataMigrationRequestDTO}
   * request comes as a part of rest API which performs all the operations(extract ddl, translate
   * ddl, unload data from Sf and load BQ).
   */
  static TranslateDDLDataDTO getTranslateDDLDataDTOFromSFDataMigrationRequestDTO(
      SFDataMigrationRequestDTO sfDataMigrationRequestDTO) {
    return getTranslateDDLDataDTO(
        sfDataMigrationRequestDTO.getSourceDatabaseName(),
        sfDataMigrationRequestDTO.getSourceSchemaName(),
        sfDataMigrationRequestDTO.getTargetDatabaseName(),
        sfDataMigrationRequestDTO.getTargetSchemaName(),
        sfDataMigrationRequestDTO.getLocation(),
        sfDataMigrationRequestDTO.getGcsBucketForTranslation());
  }

  /**
   * Method to extract {@link TranslateDDLDataDTO} values from the {@link
   * SFExtractAndTranslateDDLRequestDTO} This request comes as a part of different rest API which
   * only extract DDL and translate them.
   */
  static TranslateDDLDataDTO getTranslateDDLDataDTOFromExtractDDLRequestDTO(
      SFExtractAndTranslateDDLRequestDTO extractDDLRequestDTO) {
    return getTranslateDDLDataDTO(
        extractDDLRequestDTO.getSourceDatabaseName(),
        extractDDLRequestDTO.getSourceSchemaName(),
        extractDDLRequestDTO.getTargetDatabaseName(),
        extractDDLRequestDTO.getTargetSchemaName(),
        extractDDLRequestDTO.getLocation(),
        extractDDLRequestDTO.getGcsBucketForTranslation());
  }

  /**
   * Method to extract {@link BigQueryDetailsDataDTO} values from the {@link ApplicationConfigData}
   */
  static BigQueryDetailsDataDTO migrateRequestToBigQueryDetailDataDto(
      ApplicationConfigData applicationConfigData) {
    BigQueryDetailsDataDTO bigQueryDetailsDto = new BigQueryDetailsDataDTO();
    bigQueryDetailsDto.setUniqueIdentifier(applicationConfigData.getId());
    bigQueryDetailsDto.setProjectId(applicationConfigData.getTargetDatabaseName());
    bigQueryDetailsDto.setDatasetId(applicationConfigData.getTargetSchemaName());
    bigQueryDetailsDto.setTableName(applicationConfigData.getTargetTableName());
    bigQueryDetailsDto.setBucketName(applicationConfigData.getGcsBucketForTranslation());
    bigQueryDetailsDto.setGcsDDLFilePath(applicationConfigData.getTranslatedDDLGCSPath());
    bigQueryDetailsDto.setSnowflakeDataUnloadGCSPath(
        applicationConfigData.getSnowflakeStageLocation());
    bigQueryDetailsDto.setBqLoadFileFormat(applicationConfigData.getBqLoadFileFormat());
    bigQueryDetailsDto.setLocation(applicationConfigData.getLocation());
    return bigQueryDetailsDto;
  }

  /**
   * Method to convert {@link ApplicationConfigData} values to the {@link SFDataMigrationResponse}.
   */
  static SFDataMigrationResponse applicationConfigDataToSFSfDataMigrationResponse(
      ApplicationConfigData applicationConfigData) {
    SFDataMigrationResponse sfDataMigrationResponse = new SFDataMigrationResponse();
    sfDataMigrationResponse.setSourceDatabaseName(applicationConfigData.getSourceDatabaseName());
    sfDataMigrationResponse.setSourceSchemaName(applicationConfigData.getSourceSchemaName());
    sfDataMigrationResponse.setSourceTableName(applicationConfigData.getSourceTableName());
    sfDataMigrationResponse.setTableDDLExtracted(applicationConfigData.isSourceDDLCopied());
    sfDataMigrationResponse.setTableDDLTranslated(applicationConfigData.isTranslatedDDLCopied());
    sfDataMigrationResponse.setTableDataUnloadedFromSnowflake(
        applicationConfigData.isDataUnloadedFromSnowflake());
    sfDataMigrationResponse.setBQTableCreated(applicationConfigData.isBQTableCreated());
    sfDataMigrationResponse.setTableDataLoadedInBQ(applicationConfigData.isDataLoadedInBQ());
    sfDataMigrationResponse.setTableProcessingDone(applicationConfigData.isRowProcessingDone());
    sfDataMigrationResponse.setCreatedTime(applicationConfigData.getCreatedTime());
    sfDataMigrationResponse.setLastUpdatedTime(applicationConfigData.getLastUpdatedTime());
    sfDataMigrationResponse.setSuccess(applicationConfigData.isRowProcessingDone());
    sfDataMigrationResponse.setRequestLogId(applicationConfigData.getRequestLogId());
    return sfDataMigrationResponse;
  }

  /** This method just a helper method for above code so that duplicate code gets avoided. */
  private static DDLDataDTO getDdlDataDTO(
      String sourceDatabaseName,
      String sourceSchemaName,
      String targetDatabaseName,
      String targetSchemaName,
      String sourceTableName,
      boolean schema) {
    DDLDataDTO ddlDataDTO = new DDLDataDTO();

    ddlDataDTO.setSourceDatabaseName(sourceDatabaseName);
    ddlDataDTO.setSourceSchemaName(sourceSchemaName);
    ddlDataDTO.setTargetDatabaseName(targetDatabaseName);
    ddlDataDTO.setTargetSchemaName(targetSchemaName);
    ddlDataDTO.setSourceTableName(sourceTableName);
    ddlDataDTO.setSchema(schema);
    return ddlDataDTO;
  }

  /** This method just a helper method for above code so that duplicate code gets avoided. */
  private static GCSDetailsDataDTO getGcsDetailsDataDTO(
      String sourceDatabaseName,
      String sourceSchemaName,
      String sourceTableName,
      String targetDatabaseName,
      String targetSchemaName,
      String gcsBucketForDDLs) {
    GCSDetailsDataDTO gcsDetailsDataDTO = new GCSDetailsDataDTO();

    gcsDetailsDataDTO.setSourceDatabaseName(sourceDatabaseName);
    gcsDetailsDataDTO.setSourceSchemaName(sourceSchemaName);
    gcsDetailsDataDTO.setSourceTableName(sourceTableName);
    gcsDetailsDataDTO.setTargetDatabaseName(targetDatabaseName);
    gcsDetailsDataDTO.setTargetSchemaName(targetSchemaName);
    gcsDetailsDataDTO.setGcsBucketForDDLs(gcsBucketForDDLs);
    return gcsDetailsDataDTO;
  }

  /** This method is just a helper method for above code so that duplicate code gets avoided. */
  private static TranslateDDLDataDTO getTranslateDDLDataDTO(
      String sourceDatabaseName,
      String sourceSchemaName,
      String targetDatabaseName,
      String targetSchemaName,
      String translationJobLocation,
      String gcsBucketForTranslation) {
    TranslateDDLDataDTO translateDDLDataDTO = new TranslateDDLDataDTO();

    translateDDLDataDTO.setSourceDatabaseName(sourceDatabaseName);
    translateDDLDataDTO.setSourceSchemaName(sourceSchemaName);
    translateDDLDataDTO.setTargetDatabaseName(targetDatabaseName);
    translateDDLDataDTO.setTargetSchemaName(targetSchemaName);
    translateDDLDataDTO.setTranslationJobLocation(translationJobLocation);
    translateDDLDataDTO.setGcsBucketForTranslation(gcsBucketForTranslation);
    return translateDDLDataDTO;
  }

  /**
   * Method to convert {@link ApplicationConfigData} values to the {@link
   * SnowflakeUnloadToGCSDataDTO}.
   */
  static SnowflakeUnloadToGCSDataDTO applicationConfigDataToSnowflakeUnloadToGCSDataDTO(
      ApplicationConfigData applicationConfigData) {
    SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO = new SnowflakeUnloadToGCSDataDTO();
    snowflakeUnloadToGCSDataDTO.setDatabaseName(applicationConfigData.getSourceDatabaseName());
    snowflakeUnloadToGCSDataDTO.setSchemaName(applicationConfigData.getSourceSchemaName());
    snowflakeUnloadToGCSDataDTO.setTableName(applicationConfigData.getSourceTableName());
    snowflakeUnloadToGCSDataDTO.setWarehouse(applicationConfigData.getWarehouse());
    snowflakeUnloadToGCSDataDTO.setSnowflakeStageLocation(
        applicationConfigData.getSnowflakeStageLocation());
    snowflakeUnloadToGCSDataDTO.setSnowflakeFileFormatValue(
        applicationConfigData.getSnowflakeFileFormatValue());
    return snowflakeUnloadToGCSDataDTO;
  }

  /**
   * Method to convert {@link SnowflakeUnloadToGCSRequestDTO} values to the {@link
   * SnowflakeUnloadToGCSDataDTO}.
   */
  static SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSRequestToSnowflakeUnloadToGCSDataDTO(
      String tableName, SnowflakeUnloadToGCSRequestDTO snowflakeUnloadToGCSRequestDTO) {
    SnowflakeUnloadToGCSDataDTO snowflakeUnloadToGCSDataDTO = new SnowflakeUnloadToGCSDataDTO();
    snowflakeUnloadToGCSDataDTO.setDatabaseName(
        snowflakeUnloadToGCSRequestDTO.getSourceDatabaseName());
    snowflakeUnloadToGCSDataDTO.setSchemaName(snowflakeUnloadToGCSRequestDTO.getSourceSchemaName());
    snowflakeUnloadToGCSDataDTO.setTableName(tableName);
    snowflakeUnloadToGCSDataDTO.setWarehouse(snowflakeUnloadToGCSRequestDTO.getWarehouse());
    snowflakeUnloadToGCSDataDTO.setSnowflakeStageLocation(
        snowflakeUnloadToGCSRequestDTO.getSnowflakeStageLocation());
    snowflakeUnloadToGCSDataDTO.setSnowflakeFileFormatValue(
        snowflakeUnloadToGCSRequestDTO.getSnowflakeFileFormatValue());
    return snowflakeUnloadToGCSDataDTO;
  }

  /** Creating a copy of {@link DDLDataDTO} */
  static DDLDataDTO cloneDDLDataDTO(DDLDataDTO ddlDataDTO) {
    DDLDataDTO ddlDataDTO1 = new DDLDataDTO();
    ddlDataDTO1.setDatabase(ddlDataDTO.isDatabase());
    ddlDataDTO1.setSourceSchemaName(ddlDataDTO.getSourceSchemaName());
    ddlDataDTO1.setSourceDatabaseName(ddlDataDTO.getSourceDatabaseName());
    ddlDataDTO1.setSourceTableName(ddlDataDTO.getSourceTableName());
    ddlDataDTO1.setSchema(ddlDataDTO.isSchema());
    ddlDataDTO1.setTargetDatabaseName(ddlDataDTO.getTargetDatabaseName());
    ddlDataDTO1.setTargetSchemaName(ddlDataDTO.getTargetSchemaName());
    return ddlDataDTO1;
  }
}
