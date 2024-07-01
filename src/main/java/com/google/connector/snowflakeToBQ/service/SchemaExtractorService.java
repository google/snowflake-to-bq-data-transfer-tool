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

import static com.google.connector.snowflakeToBQ.util.ErrorCode.TABLE_NAME_NOT_PRESENT_IN_REQUEST;

import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.DDLDataDTO;
import com.google.connector.snowflakeToBQ.repository.SnowflakesJdbcDataRepository;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/** Class to extract the ddls from snowflakes based on received parameter. */
@Service
public class SchemaExtractorService {
  private static final Logger log = LoggerFactory.getLogger(SchemaExtractorService.class);
  private final SnowflakesJdbcDataRepository jdbcRepository;

  @Autowired
  public SchemaExtractorService(SnowflakesJdbcDataRepository jdbcRepository) {
    this.jdbcRepository = jdbcRepository;
  }

  /**
   * Method to extract DDLs from the Snowflake based on the input received as a part {@link
   * DDLDataDTO}
   *
   * @param ddlDataDTO DTO which contains input values required for extracting the DDLs.
   * @return Map where key is the table name and value is the Snowflake ddl for the corresponding
   *     table.
   */
  public Map<String, String> getDDLs(DDLDataDTO ddlDataDTO) {
    // Set the JDBC template if its null
    jdbcRepository.setJdbcTemplate(
        ddlDataDTO.getSourceDatabaseName(), ddlDataDTO.getSourceSchemaName());
    // If the request received isSchema=true, means extracts all the table's DDLs which are present
    // within the Schema, else fetch the DDLs for received table(s)
    if (ddlDataDTO.isSchema()) {
      log.info("Fetching all tables DDLs from Snowflake");
      return jdbcRepository.getAllTableDDLs(ddlDataDTO.getSourceSchemaName());
    } else {
      log.info("Fetching given table(s) DDLs from Snowflake");
      if (StringUtils.isBlank(ddlDataDTO.getSourceTableName())) {
        throw new SnowflakeConnectorException(
            TABLE_NAME_NOT_PRESENT_IN_REQUEST.getMessage(),
            TABLE_NAME_NOT_PRESENT_IN_REQUEST.getErrorCode());
      }
      String[] tableNames = ddlDataDTO.getSourceTableName().split(",");
      log.info("Received table count:{}", tableNames.length);
      return jdbcRepository.multipleTableDDLS(tableNames);
    }
  }
}
