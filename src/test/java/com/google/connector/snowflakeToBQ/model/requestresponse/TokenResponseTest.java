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

package com.google.connector.snowflakeToBQ.model.requestresponse;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.model.response.TokenResponse;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TokenResponseTest extends AbstractTestBase {

  @Test
  public void testTokenResponse() {
    // Create a TokenResponse object
    TokenResponse tokenResponse =
        new TokenResponse()
            .setAccessToken("accessToken")
            .setTokenType("tokenType")
            .setExpiresInSeconds(3600)
            .setRefreshToken("refreshToken")
            .setScope("scope");

    // Assert the values using getter methods
    Assert.assertEquals("accessToken", tokenResponse.getAccessToken());
    Assert.assertEquals("tokenType", tokenResponse.getTokenType());
    Assert.assertEquals(3600, tokenResponse.getExpiresInSeconds());
    Assert.assertEquals("refreshToken", tokenResponse.getRefreshToken());
    Assert.assertEquals("scope", tokenResponse.getScope());
  }

  @Ignore
  @Test
  public void testTokenResponseScenario1() {
    // Create a TokenResponse object
    TokenResponse tokenResponse =
        new TokenResponse()
            .setAccessToken(null)
            .setTokenType("tokenType")
            .setExpiresInSeconds(3600)
            .setRefreshToken("refreshToken")
            .setScope("scope");

    // Assert the values using getter methods
    Assert.assertEquals("accessToken", tokenResponse.getAccessToken());
    Assert.assertEquals("tokenType", tokenResponse.getTokenType());
    Assert.assertEquals(3600, tokenResponse.getExpiresInSeconds());
    Assert.assertEquals("refreshToken", tokenResponse.getRefreshToken());
    Assert.assertEquals("scope", tokenResponse.getScope());
  }
}
