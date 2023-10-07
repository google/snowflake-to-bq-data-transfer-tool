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

package com.google.connector.snowflakeToBQ.model.response;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Key;
import com.google.api.client.util.Preconditions;

public class TokenResponse extends GenericJson {
  @Key("access_token")
  private String accessToken;

  @Key("token_type")
  private String tokenType;

  @Key("expires_in")
  private int expiresInSeconds;

  @Key("refresh_token")
  private String refreshToken;

  @Key private String scope;

  public TokenResponse() {}

  public final String getAccessToken() {
    return this.accessToken;
  }

  public TokenResponse setAccessToken(String accessToken) {
    this.accessToken = Preconditions.checkNotNull(accessToken);
    return this;
  }

  public final String getTokenType() {
    return this.tokenType;
  }

  public TokenResponse setTokenType(String tokenType) {
    this.tokenType = Preconditions.checkNotNull(tokenType);
    return this;
  }

  public final int getExpiresInSeconds() {
    return this.expiresInSeconds;
  }

  public TokenResponse setExpiresInSeconds(int expiresInSeconds) {
    this.expiresInSeconds = expiresInSeconds;
    return this;
  }

  public final String getRefreshToken() {
    return this.refreshToken;
  }

  public TokenResponse setRefreshToken(String refreshToken) {
    this.refreshToken = refreshToken;
    return this;
  }

  public final String getScope() {
    return this.scope;
  }

  public TokenResponse setScope(String scope) {
    this.scope = scope;
    return this;
  }
}
