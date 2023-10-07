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

package com.google.connector.snowflakeToBQ.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * This class creates custom thread pool executor bean. This thread pool will be used by Async
 * threads. Max pool size is configuration via application.properties file.
 */
@Configuration
public class CustomAsyncConfig {

  @Value("${custom.thread.executor.max.pool.size}")
  private int customThreadExecutorMaxPoolSize;

  @Bean(name = "customExecutor")
  public ThreadPoolTaskExecutor customExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    // This many (5) thread will be ready to take task anytime in the pool
    executor.setCorePoolSize(5);
    // This many (5) thread max will be executing tasks in the pool
    executor.setMaxPoolSize(customThreadExecutorMaxPoolSize);
    // This many thread can queue up and wait in the pool, beyond this new thread will be rejected.
    executor.setQueueCapacity(40);
    executor.setThreadNamePrefix("custom-async-");
    executor.initialize();
    return executor;
  }
}
