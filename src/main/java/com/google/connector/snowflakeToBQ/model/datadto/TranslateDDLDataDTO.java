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

package com.google.connector.snowflakeToBQ.model.datadto;

import lombok.Getter;
import lombok.Setter;

/** Class which will be used by services and hold the data for translation related operations. */
@Setter
@Getter
public class TranslateDDLDataDTO extends CommonDataDTO {
  private String gcsBucketForTranslation;
  private String translationJobLocation;

  @Override
  public String toString() {
    String commonDTOString = super.toString();
    return "TranslateDDLDataDTO{"
        + "gcsBucketForTranslation='"
        + gcsBucketForTranslation
        + '\''
        + ", translationJobLocation='"
        + translationJobLocation
        + '\''
        + ", "
        + commonDTOString
        + '}';
  }
}
