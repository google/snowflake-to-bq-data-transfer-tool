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

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.entity.ApplicationConfigData;
import com.google.connector.snowflakeToBQ.repository.ApplicationConfigDataRepository;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import com.google.connector.snowflakeToBQ.util.PropertyManager;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.event.annotation.BeforeTestMethod;

import static com.google.connector.snowflakeToBQ.util.PropertyManager.OUTPUT_FORMATTER1;

public class ApplicationConfigDataRepositoryTest extends AbstractTestBase {

  @Autowired ApplicationConfigDataService applicationConfigDataService;

  @Autowired ApplicationConfigDataRepository applicationConfigDataRepository;

  @BeforeTestMethod
  public void cleanup() {
    applicationConfigDataRepository.deleteAll();
  }

  @Test()
  public void testSaveApplicationConfigDataService() {
    ApplicationConfigData applicationConfigData = new ApplicationConfigData();
    applicationConfigData.setSourceSchemaName("public");
    applicationConfigData.setSourceTableName("source_table");
    applicationConfigData.setLastUpdatedTime(
            PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
    ApplicationConfigData data =
        applicationConfigDataService.saveApplicationConfigDataService(applicationConfigData);
    Assert.assertNotNull(data.getId());
  }

  @Test()
  public void testSaveAllApplicationConfigDataServices() {
    ApplicationConfigData applicationConfigData = new ApplicationConfigData();
    applicationConfigData.setSourceSchemaName("public");
    applicationConfigData.setSourceTableName("source_table");
    applicationConfigData.setLastUpdatedTime(
            PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
    List<ApplicationConfigData> applicationConfigDataList = new ArrayList<>();
    applicationConfigDataList.add(applicationConfigData);

    List<ApplicationConfigData> data =
        applicationConfigDataService.saveAllApplicationConfigDataServices(
            applicationConfigDataList);
    Assert.assertEquals(1, data.size());
  }

  @Test()
  public void testGetApplicationConfigDataById() {
    ApplicationConfigData applicationConfigData = new ApplicationConfigData();
    applicationConfigData.setSourceSchemaName("public");
    applicationConfigData.setSourceTableName("source_table");
    applicationConfigData.setLastUpdatedTime(
            PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
    ApplicationConfigData data =
        applicationConfigDataService.saveApplicationConfigDataService(applicationConfigData);
    ApplicationConfigData returnedData =
        applicationConfigDataService.getApplicationConfigDataById(data.getId());
    Assert.assertEquals(data.getId(), returnedData.getId());
    Assert.assertEquals(data.getSourceSchemaName(), returnedData.getSourceSchemaName());
  }

  @Test()
  public void testGetApplicationConfigDataByIdNegativeScenario() {
    ApplicationConfigData returnedData =
        applicationConfigDataService.getApplicationConfigDataById(10000000L);
    Assert.assertNull(returnedData);
  }

  @Test()
  public void testFindByColumnName() {
    ApplicationConfigData applicationConfigData = new ApplicationConfigData();
    applicationConfigData.setSourceSchemaName("public");
    applicationConfigData.setSourceTableName("source_table");
    applicationConfigData.setRowProcessingDone(true);
    applicationConfigData.setLastUpdatedTime(
            PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
    ApplicationConfigData data =
        applicationConfigDataService.saveApplicationConfigDataService(applicationConfigData);
    List<ApplicationConfigData> dataList = applicationConfigDataService.findByColumnName(true);
    Assert.assertEquals(1, dataList.size());
  }

  @Test()
  public void testFindByColumnNameNoDataFound() {
    List<ApplicationConfigData> data = applicationConfigDataService.findByColumnName(true);
    Assert.assertEquals(0, data.size());
  }
}
