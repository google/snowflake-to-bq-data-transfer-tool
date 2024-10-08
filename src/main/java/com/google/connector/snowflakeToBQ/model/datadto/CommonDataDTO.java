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

package com.google.connector.snowflakeToBQ.model.datadto;

import lombok.Getter;
import lombok.Setter;

/**
 * Class which will be used by services and hold the common data. Class which require this data
 * extends this class
 */
@Getter
@Setter
public class CommonDataDTO {
  private String sourceDatabaseName;
  private String sourceSchemaName;
  private String targetDatabaseName;
  private String targetSchemaName;

  @Override
  public String toString() {
    return "CommonDataDTO{"
        + "sourceDatabaseName='"
        + sourceDatabaseName
        + '\''
        + ", sourceSchemaName='"
        + sourceSchemaName
        + '\''
        + ", targetDatabaseName='"
        + targetDatabaseName
        + '\''
        + ", targetSchemaName='"
        + targetSchemaName
        + '\''
        + '}';
  }
}
