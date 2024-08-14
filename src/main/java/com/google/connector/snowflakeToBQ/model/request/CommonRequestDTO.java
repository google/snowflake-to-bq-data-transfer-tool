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
import lombok.Getter;
import lombok.Setter;

/**
 * Class which will be used to transfer data received as input request to the service after mapping
 * it to Data DTOs. This class holds the common request parameters which gets used by other classes.
 * It also helps in validating the input request.
 */
@Getter
@Setter
public class CommonRequestDTO {
  @NotBlank(message = "Source database name can not be empty")
  private String sourceDatabaseName;

  @NotBlank(message = "Source schema name can not be empty")
  private String sourceSchemaName;

  @NotBlank(message = "Source table name can not be empty")
  private String sourceTableName;

  // TODO decide if we need to implement isSchema for SnowflakeUnload request and based on that add
  // validation, current its commented as its a common property.
  private boolean schema;

  @NotBlank(message = "warehouse can not be empty")
  private String warehouse;

  private String location;

  @Override
  public String toString() {
    return "CommonRequestDTO{"
        + "sourceDatabaseName='"
        + sourceDatabaseName
        + '\''
        + ", sourceSchemaName='"
        + sourceSchemaName
        + '\''
        + ", sourceTableName='"
        + sourceTableName
        + '\''
        + ", schema="
        + schema
        + ", warehouse='"
        + warehouse
        + '\''
        + ", location='"
        + location
        + '\''
        + '}';
  }
}
