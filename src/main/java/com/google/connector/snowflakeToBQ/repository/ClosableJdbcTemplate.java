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

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * A subclass of {@link JdbcTemplate} that implements the {@link ClosableResource} interface.
 *
 * <p>This class provides a custom implementation of {@link JdbcTemplate} with the ability to close
 * the associated {@link HikariDataSource} when the resource is no longer needed. This is
 * particularly useful when the {@link ClosableJdbcTemplate} is used in conjunction with the {@link
 * com.google.connector.snowflakeToBQ.cache.EasyCache} class, allowing the JDBC connection pool to
 * be properly cleaned up when entries are removed from the cache or when the cache is cleared.
 *
 * <p>Example usage:
 *
 * <pre>
 * ClosableJdbcTemplate jdbcTemplate = new ClosableJdbcTemplate(dataSource);
 * cache.put("myKey", jdbcTemplate);
 * </pre>
 *
 * <p>When the entry is removed from the cache, the associated {@link HikariDataSource} will be
 * closed automatically.
 */
public class ClosableJdbcTemplate extends JdbcTemplate implements ClosableResource {

  private final HikariDataSource dataSource;

  /**
   * Constructs a new {@link ClosableJdbcTemplate} with the specified {@link HikariDataSource}.
   *
   * @param dataSource the {@link HikariDataSource} to be used by this {@link JdbcTemplate}
   *     instance. The dataSource will be closed when {@link #closeResource()} is called.
   */
  public ClosableJdbcTemplate(HikariDataSource dataSource) {
    super(dataSource);
    this.dataSource = dataSource;
  }

  /**
   * Closes the associated {@link HikariDataSource}, releasing any database connections and cleaning
   * up resources.
   *
   * <p>This method is automatically called when the {@link ClosableJdbcTemplate} is removed from
   * the cache, ensuring that database connections are properly closed.
   */
  @Override
  public synchronized void closeResource() {
    dataSource.close();
  }
}
