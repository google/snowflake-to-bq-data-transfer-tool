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

package com.google.connector.snowflakeToBQ.service.instancecreator;

import static org.mockito.Mockito.*;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.service.Instancecreator.BigQueryInstanceCreator;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;

@ExtendWith(MockitoExtension.class)
@Ignore
public class BigQueryInstanceCreatorTest extends AbstractTestBase {

  @Autowired BigQueryInstanceCreator bigQueryInstanceCreator;

  @Test
  public void testLoadBigQueryJobNegative() {

    MockedStatic<BigQueryOptions> bigQueryOptions = mockStatic(BigQueryOptions.class);
    bigQueryOptions.when(BigQueryOptions::getDefaultInstance).thenReturn(BigQueryOptions.class);
    when(BigQueryOptions.getDefaultInstance().getService()).thenReturn(mock(BigQuery.class));
    com.google.cloud.bigquery.BigQueryOptions.Builder builderMock =
        mock(com.google.cloud.bigquery.BigQueryOptions.Builder.class);
    //    when(bigQueryOptions.newBuilder())
    //        .thenReturn(builderMock);
    //    when(builderMock.setCredentials(any(Credentials.class))).thenReturn(builderMock);
    //    when(builderMock.build()).thenReturn(bigQueryOptions);
    //    when(bigQueryOptions.getService()).thenReturn(mock(BigQuery.class));

    BigQuery jobStatus = bigQueryInstanceCreator.getBigQueryClient();
    //    Assert.assertFalse(jobStatus);
    System.out.println(jobStatus);
  }
}
