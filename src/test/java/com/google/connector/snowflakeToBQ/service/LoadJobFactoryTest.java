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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.service.bigqueryjoboptions.*;
import lombok.Setter;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

@Setter
public class LoadJobFactoryTest extends AbstractTestBase {
  @Autowired private LoadJobFactory loadJobFactory;

  /**
   * Method to test {@link LoadOption} CSV switch case.
   */
  @Test
  public void testCreateServiceCSVType() {
    Schema schema = Schema.of(
            Field.of("name", StandardSQLTypeName.STRING),
            Field.of("age", StandardSQLTypeName.INT64) // Mode is NULLABLE by default
    );
    LoadJobOptions csvLoadJob=  loadJobFactory.createService(LoadOption.CSV,schema);
    Assert.assertTrue(csvLoadJob instanceof CSVLoadJob);
  }

  /**
   * Method to test {@link LoadOption} PARQUET switch case.
   */
  @Test
  public void testCreateServiceParquetType() {
    Schema schema = Schema.of(
            Field.of("name", StandardSQLTypeName.STRING),
            Field.of("age", StandardSQLTypeName.INT64) // Mode is NULLABLE by default
    );
    LoadJobOptions csvLoadJob=  loadJobFactory.createService(LoadOption.PARQUET,schema);
    Assert.assertTrue(csvLoadJob instanceof ParquetLoadJob);
  }
}
