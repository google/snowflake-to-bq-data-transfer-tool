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

import com.google.connector.snowflakeToBQ.entity.ApplicationConfigData;
import com.google.connector.snowflakeToBQ.repository.ApplicationConfigDataRepository;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Class which provides the method to interact with entity. Transactional annotation will help in
 * transaction management for all the method of this class, read only property is set on read only
 * methods
 */
@Service
@Transactional
public class ApplicationConfigDataService {

  private final ApplicationConfigDataRepository applicationConfigDataRepository;

  @Autowired
  public ApplicationConfigDataService(
      ApplicationConfigDataRepository applicationConfigDataRepository) {
    this.applicationConfigDataRepository = applicationConfigDataRepository;
  }

  /**
   * Save the applicationConfigData object in the table
   *
   * @param applicationConfigData object representing a row of table
   * @return updated object
   */
  public ApplicationConfigData saveApplicationConfigDataService(
      ApplicationConfigData applicationConfigData) {

    return applicationConfigDataRepository.save(applicationConfigData);
  }

  /**
   * Save the all the applicationConfigData object in the table in one operation
   *
   * @param applicationConfigDataList list of applicationConfigData object representing more than a
   *     row of table
   * @return updated object list
   */
  public List<ApplicationConfigData> saveAllApplicationConfigDataServices(
      List<ApplicationConfigData> applicationConfigDataList) {

    return applicationConfigDataRepository.saveAllAndFlush(applicationConfigDataList);
  }

  /**
   * Getting the rows based on the id value. It will return null if Id is not found.
   *
   * @param id id column of the table
   * @return fetched object based on the id
   */
  @Transactional(readOnly = true)
  public ApplicationConfigData getApplicationConfigDataById(Long id) {
    return applicationConfigDataRepository.findById(id).orElse(null);
  }

  /**
   * Fetching rows from table based on the column value. Here column name is is_row_processing_done.
   *
   * @param value value of is_row_processing_done column either true or false.
   * @return list of object fetched.
   */
  @Transactional(readOnly = true)
  public List<ApplicationConfigData> findByColumnName(boolean value) {
    return applicationConfigDataRepository.findByIsRowProcessingDone(value);
  }

  @Transactional(readOnly = true)
  public List<ApplicationConfigData> findByIds(List<Long> ids) {
    return applicationConfigDataRepository.findAllById(ids);
  }
}
