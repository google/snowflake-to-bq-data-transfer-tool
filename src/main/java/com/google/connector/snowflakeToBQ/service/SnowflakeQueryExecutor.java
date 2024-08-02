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

import static com.google.connector.snowflakeToBQ.util.ErrorCode.JDBC_EXECUTION_EXCEPTION;

import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.mapper.MigrateRequestMapper;
import com.google.connector.snowflakeToBQ.model.datadto.DDLDataDTO;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.connector.snowflakeToBQ.repository.JdbcTemplateProvider;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** Class to execute queries on snowflake database using jdbc template. */
@Service
@Setter
public class SnowflakeQueryExecutor {
  private static final Logger log = LoggerFactory.getLogger(SnowflakeQueryExecutor.class);
  private static final String GET_DDL_QUERY = "select GET_DDL('TABLE', '%s')";
  private static final String SHOW_TABLES_QUERY = "SHOW TABLES IN SCHEMA %s";
  private final JdbcTemplateProvider jdbcTemplates;

  @Value("${jdbc.url}")
  private String url;

  @Value("${authenticator.type}")
  private String authenticatorType;

  @Autowired
  public SnowflakeQueryExecutor(JdbcTemplateProvider JdbcTemplate) {
    this.jdbcTemplates = JdbcTemplate;
  }

  /**
   * Extracts the Data Definition Language (DDL) statement for a specified table in Snowflake.
   *
   * <p>This method constructs an SQL query to retrieve the DDL statement of the table identified by
   * the given {@link DDLDataDTO}. It uses the provided database and schema names to create or
   * retrieve a corresponding {@link JdbcTemplateProvider} instance, which then executes the query.
   *
   * @param ddlDataDTO a {@link DDLDataDTO} object containing the source database name, schema name,
   *     and table name for which the DDL statement is to be extracted.
   * @return the DDL statement of the specified table as a {@link String}.
   * @throws SnowflakeConnectorException if any error occurs during the execution of the query.
   */
  public String extractDDL(DDLDataDTO ddlDataDTO) {
    String sql = String.format(GET_DDL_QUERY, ddlDataDTO.getSourceTableName());
    String result;
    try {
      result =
          jdbcTemplates
              .getOrCreateJdbcTemplate(
                  ddlDataDTO.getSourceDatabaseName(), ddlDataDTO.getSourceSchemaName())
              .queryForObject(sql, String.class);

    } catch (Exception e) {
      log.error(
          JDBC_EXECUTION_EXCEPTION.getMessage() + ", Error Message:{}\nStack Trace:",
          e.getMessage(),
          e);
      throw new SnowflakeConnectorException(
          JDBC_EXECUTION_EXCEPTION.getMessage(), JDBC_EXECUTION_EXCEPTION.getErrorCode());
    }
    log.info(
        "Table Name:{}, Extracted DDL From Snowflakes: {}",
        ddlDataDTO.getSourceTableName(),
        result);
    return result;
  }

  /**
   * Getting the tables present in the schema
   *
   * @param ddlDataDTO a {@link DDLDataDTO} object containing the source database name, schema name,
   *     and table name for which the DDL statement is to be extracted.
   * @return List of Table names
   */
  public List<DDLDataDTO> getAllTableNames(DDLDataDTO ddlDataDTO) {
    String query = String.format(SHOW_TABLES_QUERY, ddlDataDTO.getSourceSchemaName());

    try {
      return jdbcTemplates
          .getOrCreateJdbcTemplate(
              ddlDataDTO.getSourceDatabaseName(), ddlDataDTO.getSourceSchemaName())
          .query(
              query,
              (rs, rowNum) -> {
                DDLDataDTO ddlDataDTO1 = MigrateRequestMapper.cloneDDLDataDTO(ddlDataDTO);
                ddlDataDTO1.setSourceTableName(rs.getString("name"));
                return ddlDataDTO1;
              });
    } catch (Exception e) {
      log.error(
          "Error while obtaining the jdbc connection, Error Message:{}\nStack Trace:",
          e.getMessage(),
          e);
      throw new SnowflakeConnectorException(
          JDBC_EXECUTION_EXCEPTION.getMessage(), JDBC_EXECUTION_EXCEPTION.getErrorCode());
    }
  }

  /**
   * Extract the ddls of all the tables present in the Schema
   *
   * @param ddlDataDTO a {@link DDLDataDTO} object containing the source database name, schema name,
   *     and table name for which the DDL statement is to be extracted.
   * @return Map of table name and corresponding ddls.
   */
  public Map<String, String> getAllTableDDLs(DDLDataDTO ddlDataDTO) {
    Map<String, String> tableMap = new HashMap<>();
    List<DDLDataDTO> tableNames = getAllTableNames(ddlDataDTO);
    for (DDLDataDTO ddlDataDTOTemp : tableNames) {
      tableMap.put(ddlDataDTOTemp.getSourceTableName(), extractDDL(ddlDataDTOTemp));
    }
    return tableMap;
  }

  /**
   * Extract the ddls of all the tables given in the array
   *
   * @param ddlDataDTO a {@link DDLDataDTO} object containing the source database name, schema name,
   *     and table name for which the DDL statement is to be extracted.
   * @return Map of table name and corresponding ddls.
   */
  public Map<String, String> multipleTableDDLS(DDLDataDTO ddlDataDTO) {
    Map<String, String> tableMap = new HashMap<>();
    String[] tableNames = ddlDataDTO.getSourceTableName().split(",");
    log.info("Received table count:{}", tableNames.length);
    for (String tableName : tableNames) {
      DDLDataDTO ddlDataDTO1 = MigrateRequestMapper.cloneDDLDataDTO(ddlDataDTO);
      ddlDataDTO1.setSourceTableName(tableName);
      tableMap.put(tableName, extractDDL(ddlDataDTO1));
    }
    return tableMap;
  }
}
