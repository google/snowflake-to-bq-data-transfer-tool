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

package com.google.connector.snowflakeToBQ.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import java.util.HashMap;
import java.util.Map;
import lombok.Setter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Setter
public class RestAPIExecutionServiceTest extends AbstractTestBase {
  @Autowired private RestAPIExecutionService restAPIExecutionService;

  @MockBean WebClient webClientMock;

  /**
   * This test validates the positive condition of the flow where the mocked value returned in such
   * a way so that snowflake job status is considered as complete and method finishes with true
   * value.
   */
  @Test
  public void testPollDataLoadStatusSuccessFullInFirstExecution() {
    // Create mock objects for the other WebClient classes
    WebClient.RequestHeadersUriSpec requestHeadersUriSpecMock =
        Mockito.mock(WebClient.RequestHeadersUriSpec.class);
    WebClient.RequestHeadersSpec requestHeadersSpecMock =
        Mockito.mock(WebClient.RequestHeadersSpec.class);
    WebClient.ResponseSpec responseSpecMock = Mockito.mock(WebClient.ResponseSpec.class);
    Mono<String> monoMock = Mockito.mock(Mono.class);

    // Define the expected argument values for the method chain
    String expectedUrl = "https://testing.snowflakecomputing.com/api/v2/statements/";
    // Stub the behavior of the WebClient instance and its method chain
    Mockito.when(webClientMock.get()).thenReturn(requestHeadersUriSpecMock);
    Mockito.when(requestHeadersUriSpecMock.uri(anyString())).thenReturn(requestHeadersSpecMock);
    Mockito.when(requestHeadersSpecMock.header(Mockito.eq("Authorization"), anyString()))
        .thenReturn(requestHeadersSpecMock);
    Mockito.when(requestHeadersSpecMock.retrieve()).thenReturn(responseSpecMock);
    Mockito.when(responseSpecMock.bodyToMono(Mockito.eq(String.class))).thenReturn(monoMock);

    Map<String, Object> expectedResponseMap = new HashMap<>();
    expectedResponseMap.put("message", "Statement executed successfully.");
    expectedResponseMap.put("key2", 123);

    Mockito.when(monoMock.flatMap(any())).thenReturn(Mono.just(expectedResponseMap));

    boolean returnResponse =
        restAPIExecutionService.pollWithTimeout(
            expectedUrl, "01ad2571-0404-9a8c-84870006f0d2", "ver:1123adbsjdhgsad");
    Assert.assertTrue(returnResponse);
  }

  /**
   * This test validates the negative condition of the flow where if the flow does not complete in
   * given retry attempt overall method will return false value. Method picks up properties(
   * snowflake.rest.api.max.attempt=3 snowflake.rest.api.poll.duration=3) from
   * test-application.properties. The mocked value returned in such a way so that snowflake job
   * status is never considered as complete and after completing the max attempt method finishes
   * with false value.
   */
  @Test
  public void testPollDataLoadStatusWaitingAndMaxAttemptFinish() {
    // Create mock objects for the other WebClient classes
    WebClient.RequestHeadersUriSpec requestHeadersUriSpecMock =
        Mockito.mock(WebClient.RequestHeadersUriSpec.class);
    WebClient.RequestHeadersSpec requestHeadersSpecMock =
        Mockito.mock(WebClient.RequestHeadersSpec.class);
    WebClient.ResponseSpec responseSpecMock = Mockito.mock(WebClient.ResponseSpec.class);
    Mono<String> monoMock = Mockito.mock(Mono.class);

    // Define the expected argument values for the method chain
    String expectedUrl = "https://testing.snowflakecomputing.com/api/v2/statements/";
    // Stub the behavior of the WebClient instance and its method chain
    Mockito.when(webClientMock.get()).thenReturn(requestHeadersUriSpecMock);
    Mockito.when(requestHeadersUriSpecMock.uri(anyString())).thenReturn(requestHeadersSpecMock);
    Mockito.when(requestHeadersSpecMock.header(Mockito.eq("Authorization"), anyString()))
        .thenReturn(requestHeadersSpecMock);
    Mockito.when(requestHeadersSpecMock.retrieve()).thenReturn(responseSpecMock);
    Mockito.when(responseSpecMock.bodyToMono(Mockito.eq(String.class))).thenReturn(monoMock);

    Map<String, Object> expectedResponseMap = new HashMap<>();
    expectedResponseMap.put(
        "message",
        "Asynchronous execution in progress. Use provided query id to perform query monitoring and"
            + " management.");
    expectedResponseMap.put("key2", 123);

    Mockito.when(monoMock.flatMap(any())).thenReturn(Mono.just(expectedResponseMap));

    boolean returnResponse =
        restAPIExecutionService.pollWithTimeout(
            expectedUrl, "01ad2571-0404-9a8c-84870006f0d2", "ver:1123adbsjdhgsad");
    Assert.assertFalse(returnResponse);
  }

  /**
   * This test validates the retry and max polling time logic of the class. In the
   * test-application.properties, properties( snowflake.rest.api.max.attempt=3
   * snowflake.rest.api.poll.duration=3) are set and from the mocking first two status are returned
   * in such a way so that flow keep on retry and does not complete until the last attempt is
   * reached. In last attempt mock returns the correct status which will mark the flow complete. As
   * max snowflake.rest.api.poll.duration is set as 3 sec, its expected that the flow completion
   * will take around 9 sec, same is verified at the end.
   */
  @Test
  public void testPollDataLoadStatusWaitingAndFinishWithInMaxAttempt() {
    // Create mock objects for the other WebClient classes
    WebClient.RequestHeadersUriSpec requestHeadersUriSpecMock =
        Mockito.mock(WebClient.RequestHeadersUriSpec.class);
    WebClient.RequestHeadersSpec requestHeadersSpecMock =
        Mockito.mock(WebClient.RequestHeadersSpec.class);
    WebClient.ResponseSpec responseSpecMock = Mockito.mock(WebClient.ResponseSpec.class);
    Mono<String> monoMock = Mockito.mock(Mono.class);

    // Define the expected argument values for the method chain
    String expectedUrl = "https://testing.snowflakecomputing.com/api/v2/statements/";

    // Stub the behavior of the WebClient instance and its method chain
    Mockito.when(webClientMock.get()).thenReturn(requestHeadersUriSpecMock);
    Mockito.when(requestHeadersUriSpecMock.uri(anyString())).thenReturn(requestHeadersSpecMock);
    Mockito.when(requestHeadersSpecMock.header(Mockito.eq("Authorization"), anyString()))
        .thenReturn(requestHeadersSpecMock);
    Mockito.when(requestHeadersSpecMock.retrieve()).thenReturn(responseSpecMock);
    Mockito.when(responseSpecMock.bodyToMono(Mockito.eq(String.class))).thenReturn(monoMock);

    Map<String, Object> expectedResponseMap = new HashMap<>();
    expectedResponseMap.put(
        "message",
        "Asynchronous execution in progress. Use provided query id to perform query monitoring and"
            + " management.");
    expectedResponseMap.put("key2", 123);

    Map<String, Object> expectedResponseMap1 = new HashMap<>();
    expectedResponseMap1.put("message", "Statement executed successfully.");
    // First attempt and then 3 retries are expected. Last retry should give the correct message
    // which means execution is finished successfully. Prior attempt will give message to mimic that
    // processing is happening.
    Mockito.when(monoMock.flatMap(any()))
        .thenReturn(Mono.just(expectedResponseMap))
        .thenReturn(Mono.just(expectedResponseMap))
        .thenReturn(Mono.just(expectedResponseMap))
        .thenReturn(Mono.just(expectedResponseMap1));

    long startTime = System.currentTimeMillis();
    boolean returnResponse =
        restAPIExecutionService.pollWithTimeout(
            expectedUrl, "01ad2571-0404-9a8c-84870006f0d2", "ver:1123adbsjdhgsad");
    long totalTime = System.currentTimeMillis() - startTime;
    Assert.assertTrue(returnResponse);
    // Here we are also asserting that total time taken in processing should be less than the given
    // value of snowflake.rest.api.poll.duration in test-application.properties file. We have given
    // 3 attempts and max poll duration is 3 sec in properties file, so ideally it should be
    // finished with in 9 seconds

    Assert.assertTrue(totalTime < 9100);
  }

  /**
   * This test method tests the parseStatus() of class RestAPIExecutionService. It mocks JSON which
   * gets returned in actual method call and validates the parsing logic of the method. Parsed map
   * gets passed to the other chained method for processing.
   */
  @Test
  public void testPollDataLoadStatusAndParseStatus() {
    // Create mock objects for the other WebClient classes
    WebClient.RequestHeadersUriSpec requestHeadersUriSpecMock =
        Mockito.mock(WebClient.RequestHeadersUriSpec.class);
    WebClient.RequestHeadersSpec requestHeadersSpecMock =
        Mockito.mock(WebClient.RequestHeadersSpec.class);
    WebClient.ResponseSpec responseSpecMock = Mockito.mock(WebClient.ResponseSpec.class);
    Mono<String> monoMock = Mockito.mock(Mono.class);

    // Define the expected argument values for the method chain
    String expectedUrl = "https://testing.snowflakecomputing.com/api/v2/statements/";
    // Stub the behavior of the WebClient instance and its method chain
    Mockito.when(webClientMock.get()).thenReturn(requestHeadersUriSpecMock);
    Mockito.when(requestHeadersUriSpecMock.uri(anyString())).thenReturn(requestHeadersSpecMock);
    Mockito.when(requestHeadersSpecMock.header(Mockito.eq("Authorization"), anyString()))
        .thenReturn(requestHeadersSpecMock);
    Mockito.when(requestHeadersSpecMock.retrieve()).thenReturn(responseSpecMock);
    // In previous test we are returning the mock map which is the output of parseStatus(). In this
    // test we are returning the JSON output (Mono String) which is the input to the parseStatus()
    // and the output of it(map) being passed to the next chained method, it will test the
    // parseStatus().
    Mockito.when(responseSpecMock.bodyToMono(Mockito.eq(String.class)))
        .thenReturn(
            Mono.just(
                "{\"message\":\"Statement executed successfully.\","
                    + " \"statementhandle\":\"01ad2571-0404-9a8c-84870006f0d\"}"));

    boolean returnResponse =
        restAPIExecutionService.pollWithTimeout(
            expectedUrl, "01ad2571-0404-9a8c-84870006f0d2", "ver:1123adbsjdhgsad");
    Assert.assertTrue(returnResponse);
  }

  /**
   * This test method tests the error condition of the parseStatus() of class
   * RestAPIExecutionService. It mocks wrong JSON which gets returned in actual method call and due
   * to which method throws parsing error.
   */
  @Test(expected = SnowflakeConnectorException.class)
  public void testPollDataLoadStatusAndParseStatusErrorScenario() {
    // Create mock objects for the other WebClient classes
    WebClient.RequestHeadersUriSpec requestHeadersUriSpecMock =
        Mockito.mock(WebClient.RequestHeadersUriSpec.class);
    WebClient.RequestHeadersSpec requestHeadersSpecMock =
        Mockito.mock(WebClient.RequestHeadersSpec.class);
    WebClient.ResponseSpec responseSpecMock = Mockito.mock(WebClient.ResponseSpec.class);
    Mono<String> monoMock = Mockito.mock(Mono.class);

    // Define the expected argument values for the method chain
    String expectedUrl = "https://testing.snowflakecomputing.com/api/v2/statements/";
    // Stub the behavior of the WebClient instance and its method chain
    Mockito.when(webClientMock.get()).thenReturn(requestHeadersUriSpecMock);
    Mockito.when(requestHeadersUriSpecMock.uri(anyString())).thenReturn(requestHeadersSpecMock);
    Mockito.when(requestHeadersSpecMock.header(Mockito.eq("Authorization"), anyString()))
        .thenReturn(requestHeadersSpecMock);
    Mockito.when(requestHeadersSpecMock.retrieve()).thenReturn(responseSpecMock);

    // In this test we are returning the corrupt JSON output (Mono String) which is the input to the
    // parseStatus() to produce the error scenario,it will test the  parseStatus() error condition

    Mockito.when(responseSpecMock.bodyToMono(Mockito.eq(String.class)))
        .thenReturn(
            Mono.just(
                "{\"message\":\"Statement executed successfully.\","
                    + " \"statementhandle:\"01ad2571-0404-9a8c-84870006f0d\"}"));

    boolean returnResponse =
        restAPIExecutionService.pollWithTimeout(
            expectedUrl, "01ad2571-0404-9a8c-84870006f0d2", "ver:1123adbsjdhgsad");
    Assert.assertTrue(returnResponse);
  }
}
