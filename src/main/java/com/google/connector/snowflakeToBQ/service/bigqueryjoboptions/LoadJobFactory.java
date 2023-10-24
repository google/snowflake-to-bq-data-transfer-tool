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

package com.google.connector.snowflakeToBQ.service.bigqueryjoboptions;

import com.google.cloud.bigquery.Schema;
import org.springframework.stereotype.Service;

/**
 * This class act as a factory class for providing the appropriate implementation based on the
 * received {@link LoadOption} value
 */
@Service
public class LoadJobFactory {
  public LoadJobOptions createService(LoadOption option, Schema parameter) {
    switch (option) {
      case CSV:
        return new CSVLoadJob(parameter);
      case PARQUET:
        return new ParquetLoadJob(parameter);
      default:
        throw new IllegalArgumentException(
            "Invalid load option, not yet defined in the factory: " + option);
    }
  }
}
