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

import com.google.connector.snowflakeToBQ.entity.ApplicationConfigData;
import java.util.List;

import com.google.connector.snowflakeToBQ.service.SnowflakeQueryExecutor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.stereotype.Repository;

/**
 * This is the repository class to store the @{@link ApplicationConfigData} values in the H2
 * database. This class uses @{@link com.google.connector.snowflakeToBQ.config.H2DataSourceConfig}
 * datasource to connect to database.
 */
@Repository
@EnableJpaRepositories(
    basePackageClasses = {
      ApplicationConfigDataRepository.class,
      SnowflakeQueryExecutor.class
    })
public interface ApplicationConfigDataRepository
    extends JpaRepository<ApplicationConfigData, Long> {
  List<ApplicationConfigData> findByIsRowProcessingDone(boolean columnValue);
}
