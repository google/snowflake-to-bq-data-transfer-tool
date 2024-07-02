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

package com.google.connector.snowflakeToBQ.advice;

import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

/**
 * Class to handle exceptions centrally that occur during the processing of HTTP requests. It serves
 * as a global exception handler for the entire application. As we have used @{@link
 * ControllerAdvice},it exception handling methods will apply globally to all controllers(by
 * default) in the application. It helps in sending the appropriate message to the caller.
 */
@ControllerAdvice
public class GlobalExceptionHandler {

  /**
   * Method to handle {@link MethodArgumentNotValidException}. It basically occurs during the
   * validation using annotation like @NotNull or @NotBlank
   *
   * @param ex {@link MethodArgumentNotValidException}
   * @return String error message which is formed in the method along with status of
   *     HttpStatus.BAD_REQUEST
   */
  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<String> handleValidationException(MethodArgumentNotValidException ex) {
    // Handle validation errors and return an appropriate response
    String errorMessage =
        "Validation failed: " + ex.getBindingResult().getAllErrors().get(0).getDefaultMessage();
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorMessage);
  }

  /**
   * Method to handle {@link SnowflakeConnectorException}. It gets thrown during the execution
   * failure of any service method or other method in the chain of call.
   *
   * @param ex {@link MethodArgumentNotValidException}
   * @return String error message which is formed in the method along with status of
   *     HttpStatus.INTERNAL_SERVER_ERROR
   */
  @ExceptionHandler(SnowflakeConnectorException.class)
  public ResponseEntity<String> handleSnowflakeConnectorException(SnowflakeConnectorException ex) {
    // Handle SnowflakeConnectorException and return an appropriate response
    String errorMessage = "Error while processing the request: " + ex.getMessage();
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorMessage);
  }
}
