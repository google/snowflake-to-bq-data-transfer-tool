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

import java.util.List;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;

/**
 * This class holds the Snowflakes unload to GCS request parameter. This is used in the flow which
 * is different from full migration flow denoted by {@link SFDataMigrationRequestDTO}
 */
@Setter
@Getter
public class SnowflakeUnloadToGCSRequestDTO {
  @NotBlank(message = "Snowflake stage location can not be blank")
  private String snowflakeStageLocation;

  @NotEmpty(message = "Table name array must not be empty")
  private List<String> tableNames;

  @NotBlank(message = "Snowflake file format can not be blank")
  private String snowflakeFileFormatValue;
}
