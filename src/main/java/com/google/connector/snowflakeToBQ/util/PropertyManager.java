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

package com.google.connector.snowflakeToBQ.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/** Class which contains the common property and methods used across application */
public class PropertyManager {
  public static DateTimeFormatter OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy_MM_dd");
  public static DateTimeFormatter OUTPUT_FORMATTER1 =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  /*below Value should not be changes as of now exactly same value is expected by migration service, else it throws erro*/
  public static String TRANSLATION_TYPE = "Translation_Snowflake2BQ";
  public static String SNOWFLAKE_STATEMENT_POST_REST_API = "/api/v2/statements/";
  public static String SNOWFLAKE_TRANSLATED_FOLDER_PREFIX = "translated-snowflake-ddls";
  public static String BACKUP_FOLDER_PREFIX = "backup-ddls";
  public static String DDL_PREFIX = "snowflake-ddls";

  public static String getDateInDesiredFormat(
      LocalDateTime localDateTime, DateTimeFormatter outputFormat) {
    return localDateTime.format(outputFormat);
  }
}
