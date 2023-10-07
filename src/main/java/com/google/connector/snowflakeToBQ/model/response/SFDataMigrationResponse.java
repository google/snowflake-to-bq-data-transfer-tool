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

package com.google.connector.snowflakeToBQ.model.response;

import lombok.Getter;
import lombok.Setter;

/** This class holds the Snowflakes Data migration response parameters. */
@Setter
@Getter
public class SFDataMigrationResponse {

  private String sourceDatabaseName;
  private String sourceSchemaName;
  private String sourceTableName;
  private boolean isTableDDLExtracted;
  private boolean isTableDDLTranslated;
  private boolean isTableDataUnloadedFromSnowflake;
  private boolean isBQTableCreated;
  private boolean isTableDataLoadedInBQ;
  private boolean isTableProcessingDone;
  private String createdTime;
  private String lastUpdatedTime;
  private boolean isSuccess;
  private String requestLogId;
}
