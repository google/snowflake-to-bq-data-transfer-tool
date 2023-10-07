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

package com.google.connector.snowflakeToBQ.config;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import javax.sql.DataSource;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class H2DataSourceConfigTest extends AbstractTestBase {
  private static final Logger log = LoggerFactory.getLogger(H2DataSourceConfigTest.class);

  @Autowired private H2DataSourceConfig h2DataSourceConfig;

  @Test()
  public void testH2DataSource() {

    Assert.assertEquals(h2DataSourceConfig.getUrl(), "jdbc:h2:file:./target/mytestdb");
    Assert.assertEquals(h2DataSourceConfig.getUsername(), "test");
    Assert.assertEquals(h2DataSourceConfig.getPassword(), "test");
    Assert.assertEquals(h2DataSourceConfig.getDriverClassName(), "org.h2.Driver");

    DataSource dataSource = h2DataSourceConfig.h2DataSource();
    Assert.assertNotNull(dataSource);
  }
}
