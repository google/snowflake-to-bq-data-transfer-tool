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

package com.google.connector.snowflakeToBQ.service.bigqueryjoboptions;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;

/** Class to create the {@link com.google.cloud.bigquery.LoadConfiguration} for PARQUET format. */
public class ParquetLoadJob implements LoadJobOptions {

  private Schema schema;

  public ParquetLoadJob(Schema schema) {
    this.schema = schema;
  }

  /**
   * Method to create {@link com.google.cloud.bigquery.LoadConfiguration} for Parquet format.
   *
   * @param tableId {@link TableId} for a table
   * @param sourceURI Path of the file in GCS containing the data.
   * @return LoadConfiguration
   */
  @Override
  public LoadJobConfiguration createLoadJob(TableId tableId, String sourceURI) {
    return LoadJobConfiguration.newBuilder(tableId, sourceURI)
        .setFormatOptions(FormatOptions.parquet())
        .setSchema(schema)
        .build();
  }
}
