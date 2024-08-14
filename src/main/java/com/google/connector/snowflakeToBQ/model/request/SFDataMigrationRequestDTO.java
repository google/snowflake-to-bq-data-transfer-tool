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

package com.google.connector.snowflakeToBQ.model.request;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

/**
 * This class holds the Snowflakes Data migration request parameter which are not present in common
 * request dto.
 */
@Setter
@Getter
public class SFDataMigrationRequestDTO extends SFExtractAndTranslateDDLRequestDTO {

  @NotNull(message = "The isBQTableExits property must be true/false")
  private boolean bqTableExists;

  @NotBlank(message = "Snowflake stage location can not be blank")
  private String snowflakeStageLocation;

  @NotBlank(message = "BigQuery load format can not be blank")
  private String bqLoadFileFormat;

  @NotBlank(message = "Snowflake file format can not be blank")
  private String snowflakeFileFormatValue;

  @Override
  public String toString() {
    String sfExtractAndTranslateDDLString = super.toString();
    return "SFDataMigrationRequestDTO{"
        + "bqTableExists="
        + bqTableExists
        + ", snowflakeStageLocation='"
        + snowflakeStageLocation
        + '\''
        + ", "
        + sfExtractAndTranslateDDLString
        + '}';
  }
}
