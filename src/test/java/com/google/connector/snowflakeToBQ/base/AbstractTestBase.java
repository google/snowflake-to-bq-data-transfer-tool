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

package com.google.connector.snowflakeToBQ.base;

import com.google.connector.snowflakeToBQ.cache.EasyCacheTest;
import com.google.connector.snowflakeToBQ.config.H2DataSourceConfigTest;
import com.google.connector.snowflakeToBQ.controller.SnowflakesConnectorControllerTest;
import com.google.connector.snowflakeToBQ.entity.ApplicationConfigDataTest;
import com.google.connector.snowflakeToBQ.mapper.MigrateRequestMapperTest;
import com.google.connector.snowflakeToBQ.model.requestresponse.TokenResponseTest;
import com.google.connector.snowflakeToBQ.repository.JDBCTemplatesTest;
import com.google.connector.snowflakeToBQ.service.SnowflakeQueryExecutorTest;
import com.google.connector.snowflakeToBQ.service.*;
import com.google.connector.snowflakeToBQ.util.PropertyManagerTest;
import com.google.connector.snowflakeToBQ.util.encryption.EncryptionValuesTest;
import org.junit.runner.RunWith;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@TestPropertySource(locations = "classpath:test-application.properties")
@TestConfiguration
@ComponentScan("com.google.connector.snowflakeToBQ")
@EnableConfigurationProperties
@SpringBootTest(
    classes = {
      SchemaExtractorServiceTest.class,
      SnowflakesServiceTest.class,
      SnowflakeMigrateDataServiceTest.class,
      RestAPIExecutionServiceTest.class,
      ApplicationConfigDataRepositoryTest.class,
      BigQueryOperationsServiceTest.class,
      WorkflowMigrationServiceTest.class,
      GoogleCloudStorageServiceTest.class,
      H2DataSourceConfigTest.class,
      SnowflakesConnectorControllerTest.class,
      ApplicationConfigDataTest.class,
      MigrateRequestMapperTest.class,
      TokenResponseTest.class,
      SnowflakeQueryExecutorTest.class,
      EncryptionValuesTest.class,
      PropertyManagerTest.class,
      ScheduledTokenRefreshServiceTest.class,
      EasyCacheTest.class,
      JDBCTemplatesTest.class
    })
public abstract class AbstractTestBase {}
