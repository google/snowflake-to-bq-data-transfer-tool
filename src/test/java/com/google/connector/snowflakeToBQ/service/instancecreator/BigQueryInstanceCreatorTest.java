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
