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

import static com.google.connector.snowflakeToBQ.util.ErrorCode.DDL_EXTRACTION_EXCEPTION;
import static com.google.connector.snowflakeToBQ.util.PropertyManager.OUTPUT_FORMATTER1;

import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.mapper.MigrateRequestMapper;
import com.google.connector.snowflakeToBQ.model.datadto.DDLDataDTO;
import com.google.connector.snowflakeToBQ.model.datadto.GCSDetailsDataDTO;
import com.google.connector.snowflakeToBQ.model.datadto.TranslateDDLDataDTO;
import com.google.connector.snowflakeToBQ.model.request.SFExtractAndTranslateDDLRequestDTO;
import com.google.connector.snowflakeToBQ.model.response.WorkflowMigrationResponse;
import com.google.connector.snowflakeToBQ.util.PropertyManager;
import java.time.LocalDateTime;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** This is service helps in extracting the DDLs from Snowflake and translating it to BQ format. */
@Service
public class ExtractAndTranslateDDLService {
  private static final Logger log = LoggerFactory.getLogger(ExtractAndTranslateDDLService.class);
  final SchemaExtractorService schemaExtractorService;
  final GoogleCloudStorageService googleCloudStorageService;
  final WorkflowMigrationService workflowMigrationService;

  public ExtractAndTranslateDDLService(
      SchemaExtractorService schemaExtractorService,
      GoogleCloudStorageService googleCloudStorageService,
      WorkflowMigrationService workflowMigrationService) {
    this.schemaExtractorService = schemaExtractorService;
    this.googleCloudStorageService = googleCloudStorageService;
    this.workflowMigrationService = workflowMigrationService;
  }

  /**
   * Method to extract and translate DDLs.
   *
   * @param extractDDLRequestDTO DTO containing the input value for performing the extraction and
   *     translation
   * @return String message to mark the execution completion.
   */
  public String extractAndTranslateDDLs(SFExtractAndTranslateDDLRequestDTO extractDDLRequestDTO) {
    try {
      DDLDataDTO ddlDataDTO =
          MigrateRequestMapper.getDDLDataDTOFromExtractDDLRequestDTO(extractDDLRequestDTO);

      // Extracting all the DDLS for the tables which are required as a part of received request.
      Map<String, String> ddls = schemaExtractorService.getDDLs(ddlDataDTO);

      GCSDetailsDataDTO gcsDetailsDataDTO =
          MigrateRequestMapper.getGCSDetailsDataDTOFromExtractDDLRequestDTO(extractDDLRequestDTO);
      // Writing ddls to GCS
      googleCloudStorageService.writeToGCS(ddls, gcsDetailsDataDTO);

      TranslateDDLDataDTO translateDDLDataDTO =
          MigrateRequestMapper.getTranslateDDLDataDTOFromExtractDDLRequestDTO(extractDDLRequestDTO);

      // Creating the migration workflow
      WorkflowMigrationResponse workflowMigrationResponse =
          workflowMigrationService.createMigrationWorkflow(translateDDLDataDTO);

      boolean returnValue =
          workflowMigrationService.isMigrationWorkflowCompleted(
              workflowMigrationResponse.getWorkflowName());

      if (returnValue) {
        return String.format(
            "Extract & translate DDL request completed successfully at %s",
            PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
      } else {
        throw new RuntimeException("Workflow could not complete");
      }
    } catch (Exception e) {
      log.error("Error while performing extractAndTranslateDDLs request,{}", e.getMessage());
      throw new SnowflakeConnectorException(
          DDL_EXTRACTION_EXCEPTION.getMessage(), DDL_EXTRACTION_EXCEPTION.getErrorCode());
    }
  }
}
