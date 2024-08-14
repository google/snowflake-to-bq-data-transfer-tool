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

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotBlank;

/**
 * This class holds Snowflake extract ddl and translate request parameters. It's used in Extract and
 * Translate DDL flow
 */
@Setter
@Getter
public class SFExtractAndTranslateDDLRequestDTO extends CommonRequestDTO {

  @NotBlank(message = "GCS bucket name for DDLs can not be empty")
  private String gcsBucketForDDLs;

  @NotBlank(message = "target database name can not be empty")
  private String targetDatabaseName;

  @NotBlank(message = "Target schema name can not be empty")
  private String targetSchemaName;

  @NotBlank(message = "GCS bucket for  translated DDLs can not be empty")
  private String gcsBucketForTranslation;

  @Override
  public String toString() {
    String commonRequestDTO = super.toString();
    return "SFExtractAndTranslateDDLRequestDTO{"
        + "gcsBucketForDDLs='"
        + gcsBucketForDDLs
        + '\''
        + ", targetDatabaseName='"
        + targetDatabaseName
        + '\''
        + ", targetSchemaName='"
        + targetSchemaName
        + '\''
        + ", gcsBucketForTranslation='"
        + gcsBucketForTranslation
        + +'\''
        + ", "
        + commonRequestDTO
        + '}';
  }
}
