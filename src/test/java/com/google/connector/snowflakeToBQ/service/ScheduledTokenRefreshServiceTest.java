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

import static org.mockito.Mockito.when;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.model.response.TokenResponse;
import com.google.connector.snowflakeToBQ.scheduler.ScheduledTokenRefreshService;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(
    properties = {
      "token.refresh.scheduler.initial.delay=3000",
      "token.refresh.scheduler.fixed.rate=4000"
    })
public class ScheduledTokenRefreshServiceTest extends AbstractTestBase {

  @MockBean private TokenRefreshService tokenRefreshService;

  @Autowired ScheduledTokenRefreshService scheduledTokenRefreshService;

  @Test
  public void testRefreshAccessTokenSchedule() throws InterruptedException {
    TokenResponse response = new TokenResponse();
    response.setRefreshToken("wewewewe12313123123");
    when(tokenRefreshService.refreshToken()).thenReturn(response);
    Thread.sleep(9000);
    // in test-application initial delay, fixed rate is set as longer duration to avoid any failure
    // when actual scheduler calls the token refresh.
    // IN this class those values are overridden using @TestPropertySource annotation and initial
    // delay isset as 3 sec and fixed rate is set as 4 sec, hence
    // actual scheduler will call it 2 times.
    Mockito.verify(tokenRefreshService, Mockito.times(2)).refreshToken();
  }
}
