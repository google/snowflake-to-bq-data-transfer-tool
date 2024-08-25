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

package com.google.connector.snowflakeToBQ.repository;

import static org.mockito.Mockito.*;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Before;
import org.junit.Test;

public class ClosableJdbcTemplateTest extends AbstractTestBase {

  private HikariDataSource mockDataSource;
  private ClosableJdbcTemplate closableJdbcTemplate;

  @Before
  public void setUp() {
    // Create a mock HikariDataSource
    mockDataSource = mock(HikariDataSource.class);
    // Create an instance of ClosableJdbcTemplate with the mock data source
    closableJdbcTemplate = new ClosableJdbcTemplate(mockDataSource);
  }

  @Test
  public void testCloseResource() {
    // Call the closeResource method
    closableJdbcTemplate.closeResource();

    // Verify that the close method on the mock data source was called
    verify(mockDataSource).close();
  }

  @Test
  public void testDataSourceIsNotClosedMultipleTimes() {
    // Call the closeResource method twice
    closableJdbcTemplate.closeResource();

    // Verify that the close method on the mock data source was called only once
    verify(mockDataSource, times(1)).close();
  }
}
