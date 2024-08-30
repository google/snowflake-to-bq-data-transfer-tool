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

package com.google.connector.snowflakeToBQ.config;

import io.netty.channel.ChannelOption;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/**
 * Configuration class for setting up a custom {@link WebClient} with specific connection pooling
 * and HTTP client options.
 *
 * <p>This configuration class defines a {@link WebClient} bean with custom settings for connection
 * pooling and timeouts using Reactor Netty.
 */
@Configuration
public class WebClientConfig {

  /*
   Sets the maximum number of connections that the connection pool can hold
   at any given time.
  */
  @Value("${reactor.netty.max.pool.size}")
  private int maxPoolSize;

  /*
   Defines how long a connection can remain idle (unused) in the pool before
   it's eligible for eviction (closure).
  */
  @Value("${reactor.netty.max.idle.time}")
  private int maxIdleTimeout;

  /*
   Sets the connection timeout for establishing a connection to the server.
   If the connection cannot be established within this time, it will time out.
  */
  @Value("${reactor.netty.connection.timeout}")
  private int connectionTimeOutToSnowflake;

  /**
   * Creates and configures a {@link WebClient} bean with a custom {@link ConnectionProvider} and
   * {@link HttpClient}.
   *
   * <p>The custom {@link ConnectionProvider} sets the maximum number of connections in the pool,
   * and specifies the maximum idle time for connections. The {@link HttpClient} is configured with
   * a connection timeout.
   *
   * @return a configured {@link WebClient} instance.
   */
  @Bean
  public WebClient webClient() {
    // Create a custom ConnectionProvider with specific configuration
    ConnectionProvider provider =
        ConnectionProvider.builder("snowflake-pool")
            .maxConnections(maxPoolSize)
            .maxIdleTime(Duration.ofMillis(maxIdleTimeout))
                .name("snowflake-pool")// Keep connections alive
            .build();

    HttpClient httpClient =
        HttpClient.create(provider)
            .option(
                ChannelOption.CONNECT_TIMEOUT_MILLIS,
                connectionTimeOutToSnowflake);

    return WebClient.builder().clientConnector(new ReactorClientHttpConnector(httpClient)).build();
  }
}
