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
package com.google.connector.snowflakeToBQ.startup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.validation.annotation.Validated;

@SpringBootApplication(scanBasePackages = "com.google.connector.snowflakeToBQ")
// This annotation helps in scanning the defined JPA repository interfaces. It helps to enable and
// configure Spring Data JPA repositories in the application.
@EnableJpaRepositories("com.google.connector.snowflakeToBQ.repository")
// This annotation helps to customize the package scanning for JPA entities. It configures the
// scanning of JPA entity classes.
@EntityScan("com.google.connector.snowflakeToBQ.entity")
// This annotation helps to enable and configure the scheduling of tasks or methods.
@EnableScheduling
// This annotation helps to enable support for asynchronous method execution
@EnableAsync
@Validated
public class SnowflakeToBqApplication {
  private static final Logger log = LoggerFactory.getLogger(SnowflakeToBqApplication.class);

  public static void main(String[] args) {
    log.info("Starting spring boot");
    SpringApplication.run(SnowflakeToBqApplication.class, args);
  }
}
