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

package com.google.connector.snowflakeToBQ.scheduler;

import com.google.connector.snowflakeToBQ.service.TokenRefreshService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * This class helps to maintain the valid oauth token. This scheduler calls the refresh token method
 * every 10 min and start after 5 min of initial startup of application.
 */
@Service
public class ScheduledTokenRefreshService {
  private static final Logger log = LoggerFactory.getLogger(ScheduledTokenRefreshService.class);
  @Autowired TokenRefreshService tokenRefreshService;

  @Scheduled(
      initialDelayString = "${token.refresh.scheduler.initial.delay}",
      fixedRateString =
          "${token.refresh.scheduler.fixed.rate}") // Refresh every 10 minutes, with initial delay
  // of 5 min (adjust as needed)
  public void refreshAccessToken() {
    tokenRefreshService.refreshToken();
    log.info("Token refresh scheduler trigger finished");
  }
}
