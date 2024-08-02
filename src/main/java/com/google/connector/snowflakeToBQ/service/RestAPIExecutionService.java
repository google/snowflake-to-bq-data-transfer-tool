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

import static com.google.connector.snowflakeToBQ.util.ErrorCode.SNOWFLAKE_RESPONSE_PARSING_ERROR;
import static com.google.connector.snowflakeToBQ.util.ErrorCode.SNOWFLAKE_REST_API_EXECUTION_ERROR;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.connector.snowflakeToBQ.config.OAuthCredentials;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.response.SnowflakeResponse;
import java.time.Duration;
import java.util.Map;

import com.google.connector.snowflakeToBQ.util.encryption.EncryptValues;
import io.grpc.netty.shaded.io.netty.channel.unix.Errors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * This helps in executing the rest API request. It basically Spring boot's WebClient(Non-blocking,
 * reactive client to perform HTTP requests, exposing a fluent, reactive API over underlying HTTP
 * client libraries such as Reactor Netty.) to execute the requests.
 */
@Service
public class RestAPIExecutionService {

  private static final Logger log = LoggerFactory.getLogger(RestAPIExecutionService.class);

  final WebClient webClient;
  private final OAuthCredentials oauthCredentials;
  private final EncryptValues encryptDecryptValues;
  private final TokenRefreshService tokenRefreshService;

  @Value("${snowflake.rest.api.max.attempt}")
  private int snowflakeRestAPIMaxAttempt;

  @Value("${snowflake.rest.api.poll.duration}")
  private int snowflakeRestAPIPollDuration;

  public RestAPIExecutionService(
      WebClient webClient,
      OAuthCredentials oauthCredentials,
      EncryptValues encryptDecryptValues,
      TokenRefreshService tokenRefreshService) {
    this.webClient = webClient;
    this.oauthCredentials = oauthCredentials;
    this.encryptDecryptValues = encryptDecryptValues;
    this.tokenRefreshService = tokenRefreshService;
  }

  /**
   * Executes a POST request to the specified URL and handles retries and error logging.
   *
   * <p>This method performs the following actions:
   *
   * <ul>
   *   <li>Validates and refreshes the OAuth token if necessary.
   *   <li>Configures and sends a POST request using the provided URL and request body.
   *   <li>Includes appropriate headers for content type and authorization.
   *   <li>Retrieves the response and converts it into a {@link SnowflakeResponse} object.
   *   <li>Handles HTTP status errors by logging the status and wrapping it in a {@link
   *       SnowflakeConnectorException}.
   *   <li>Retries the request up to 3 times with a 4-second backoff delay in case of
   *       network-related errors (e.g., {@link Errors.NativeIoException}).
   *   <li>Logs any errors that occur during the request or retries.
   * </ul>
   *
   * @param url The URL to which the POST request will be sent.
   * @param requestBody The body of the POST request, which will be sent in JSON format.
   * @return A {@link Mono} that emits the {@link SnowflakeResponse} when the request is successful.
   *     If the request fails after all retries, it will emit an error.
   * @throws Errors.NativeIoException if a network-related error occurs and the request needs to be
   *     retried.
   */
  public Mono<SnowflakeResponse> executePostAndPoll(String url, String requestBody) {
    checkAndRefreshToken();
    String accessToken =
        encryptDecryptValues.decryptValue(oauthCredentials.getOauthMap().get("accessToken"));
    return webClient
        .method(HttpMethod.POST)
        .uri(url)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
        .header("Authorization", "Bearer " + accessToken)
        .body(BodyInserters.fromValue(requestBody))
        .retrieve()
        .onStatus(
            // Below code is written to catch the errors during execution
            HttpStatus::isError,
            clientResponse -> {
              HttpStatus status = clientResponse.statusCode();
              log.error("Error while executing the rest API request: {}", status);
              return Mono.error(
                  new SnowflakeConnectorException(
                      SNOWFLAKE_REST_API_EXECUTION_ERROR.getMessage() + ", " + status,
                      SNOWFLAKE_REST_API_EXECUTION_ERROR.getErrorCode()));
            })
        .bodyToMono(SnowflakeResponse.class)
        .retryWhen(
            // TODO: Need to assess it later if these values should be moved to Property files.
            Retry.backoff(3, Duration.ofSeconds(5))
                .filter(
                    throwable -> {
                      // Retry only on specific exceptions or conditions
                      // here retrying on network-related exceptions
                      return throwable instanceof Errors.NativeIoException;
                    })
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> retrySignal.failure()))
        .doOnError(
            e -> {
              log.error("Request failed with unexpected error.\nStacktrace", e);
            });
  }

  /**
   * This method polls the rest request
   *
   * @param url Endpoint request URL
   * @param statementHandle This is the handle which Snowflake provide and can be used for tracking
   *     the status of the request in execution.
   * @return true if request completes with in the required timeout and attempt otherwise false
   */
  public boolean pollWithTimeout(String url, String statementHandle) {
    return Boolean.TRUE.equals(pollDataLoadStatus(url, statementHandle, 0).block());
  }

  /**
   * Method to poll exponential for a given number of max attempt anf executing the get rest API.
   *
   * @param url Rest API url to be executed. It should be get
   * @param statementHandle Statement handle received from Snowflake after executing the copy-into
   *     command. This helps to track the status of executing of command in Snowflake.
   * @param attempt Current executing attempt for the poll.
   * @return True if the command execution finished with in the defined max attempt and duration,
   *     False if execution could not finish.
   */
  private Mono<Boolean> pollDataLoadStatus(String url, String statementHandle, int attempt) {

    checkAndRefreshToken();
    String accessToken =
        encryptDecryptValues.decryptValue(oauthCredentials.getOauthMap().get("accessToken"));
    return webClient
        .get()
        .uri(url + statementHandle)
        // Setting the authorization token
        .header("Authorization", "Bearer " + accessToken)
        .retrieve()
        .bodyToMono(String.class)
        .flatMap(this::parseStatus)
        .flatMap(
            statusMap -> {
              log.info("Received Rest Response as map::{}", statusMap);
              // Comparing the message which snowflake return once the execution is successful
              if (statusMap.get("message").equals("Statement executed successfully.")) {
                return Mono.just(true);
                // Checking if the current attempt is less than the defined max attempts.
              } else if (attempt < snowflakeRestAPIMaxAttempt) {
                // Adding exponential delay
                int delaySeconds = (int) Math.pow(3, attempt);
                Duration delayDuration =
                    Duration.ofSeconds(Math.min(delaySeconds, snowflakeRestAPIPollDuration));

                log.info(
                    "Polling the command execution status, Attempt:{},delayDuration:{}",
                    attempt,
                    delayDuration.getSeconds());

                return Mono.delay(delayDuration)
                    .then(pollDataLoadStatus(url, statementHandle, attempt + 1));

              } else {
                // If the maximum number of attempts is reached (attempt >=
                // failure.
                return Mono.just(false); // Polling failed after maxAttempts
              }
            });
  }

  /*Helper method to parse the received response (JSON format) in to the Map  */
  private Mono<Map<String, Object>> parseStatus(String jsonStatus) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      TypeReference<Map<String, Object>> typeReference = new TypeReference<>() {};
      return Mono.just(objectMapper.readValue(jsonStatus, typeReference));
    } catch (Exception e) {
      log.error(
          "Failed to parse the received JSON response from snowflake rest API execution, error:{}\nStack Trace:",
          e.getMessage(),
          e);
      return Mono.error(
          new SnowflakeConnectorException(
              SNOWFLAKE_RESPONSE_PARSING_ERROR.getMessage(),
              SNOWFLAKE_RESPONSE_PARSING_ERROR.getErrorCode()));
    }
  }

  /**
   * Method to check if access token is set or null, in case of empty or null value it will call the
   * refresh token so that execution of request does not fail
   */
  private void checkAndRefreshToken() {
    if (oauthCredentials.getOauthMap().get("accessToken") == null
        || StringUtils.isEmpty(oauthCredentials.getOauthMap().get("accessToken").getCiphertext())) {
      tokenRefreshService.refreshToken();
    }
  }
}
