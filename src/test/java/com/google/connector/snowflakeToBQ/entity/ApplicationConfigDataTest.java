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

package com.google.connector.snowflakeToBQ.entity;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.repository.ApplicationConfigDataRepository;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.event.annotation.BeforeTestMethod;

public class ApplicationConfigDataTest extends AbstractTestBase {
  private static final Logger log = LoggerFactory.getLogger(ApplicationConfigDataTest.class);

  @Autowired private ApplicationConfigDataRepository applicationConfigDataRepository;

  @BeforeTestMethod
  public void cleanup() {
    applicationConfigDataRepository.deleteAll();
  }

  @Test()
  public void testApplicationDataEntity() {
    ApplicationConfigData applicationConfigData = new ApplicationConfigData();
    applicationConfigData.setSourceDatabaseName("SourceDB");
    applicationConfigData.setSourceSchemaName("SourceSchema");
    applicationConfigData.setSourceTableName("SourceTable");
    applicationConfigData.setTargetDatabaseName("TargetDB");
    applicationConfigData.setTargetSchemaName("TargetSchema");
    applicationConfigData.setTargetTableName("TargetTable");
    applicationConfigData.setSourceDDLCopied(true);
    applicationConfigData.setTranslatedDDLGCSPath("TranslatedDDLGCSPath");
    applicationConfigData.setTranslatedDDLCopied(true);
    applicationConfigData.setSnowflakeStageLocation("SnowflakeStageLocation");
    applicationConfigData.setBQTableCreated(true);
    applicationConfigData.setDataUnloadedFromSnowflake(true);
    applicationConfigData.setDataLoadedInBQ(true);
    applicationConfigData.setGcsBucketForDDLs("GcsBucketForDDLs");
    applicationConfigData.setGcsBucketForTranslation("GcsBucketForTranslation");
    applicationConfigData.setSchema(true);
    applicationConfigData.setLocation("Location");
    applicationConfigData.setWorkflowName("WorkflowName");
    applicationConfigData.setRowProcessingDone(true);
    applicationConfigData.setSnowflakeStatementHandle("SnowflakeStatementHandle");

    ApplicationConfigData data = applicationConfigDataRepository.save(applicationConfigData);
    Assert.assertNotNull(data.getId());
    // Verify values using getter methods
    Assert.assertEquals("SourceDB", data.getSourceDatabaseName());
    Assert.assertEquals("SourceSchema", data.getSourceSchemaName());
    Assert.assertEquals("SourceTable", data.getSourceTableName());
    Assert.assertEquals("TargetDB", data.getTargetDatabaseName());
    Assert.assertEquals("TargetSchema", data.getTargetSchemaName());
    Assert.assertEquals("TargetTable", data.getTargetTableName());
    Assert.assertTrue(data.isSourceDDLCopied());
    Assert.assertEquals("TranslatedDDLGCSPath", data.getTranslatedDDLGCSPath());
    Assert.assertTrue(data.isTranslatedDDLCopied());
    Assert.assertEquals("SnowflakeStageLocation", data.getSnowflakeStageLocation());
    Assert.assertTrue(data.isBQTableCreated());
    Assert.assertTrue(data.isDataUnloadedFromSnowflake());
    Assert.assertTrue(data.isDataLoadedInBQ());
    Assert.assertEquals("GcsBucketForDDLs", data.getGcsBucketForDDLs());
    Assert.assertEquals("GcsBucketForTranslation", data.getGcsBucketForTranslation());
    Assert.assertTrue(data.isSchema());
    Assert.assertEquals("Location", data.getLocation());
    Assert.assertEquals("WorkflowName", data.getWorkflowName());
    Assert.assertTrue(data.isRowProcessingDone());
    Assert.assertEquals("SnowflakeStatementHandle", data.getSnowflakeStatementHandle());
  }
}
