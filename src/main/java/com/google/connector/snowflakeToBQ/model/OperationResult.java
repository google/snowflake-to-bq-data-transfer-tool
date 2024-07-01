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
package com.google.connector.snowflakeToBQ.model;

public class OperationResult<T> {
  private final boolean success;
  private T result;
  private String errorMessage;

  public OperationResult(T result) {
    this.success = true;
    this.result = result;
  }

  public OperationResult(Error error) {
    this.success = false;
    this.errorMessage = error.getErrorMessage();
  }

  public boolean isSuccess() {
    return success;
  }

  public T getResult() {
    return result;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public static class Error {
    private final String errorMessage;

    public String getErrorMessage() {
      return errorMessage;
    }

    public Error(String errorMessage) {
      this.errorMessage = errorMessage;
    }
  }
}
