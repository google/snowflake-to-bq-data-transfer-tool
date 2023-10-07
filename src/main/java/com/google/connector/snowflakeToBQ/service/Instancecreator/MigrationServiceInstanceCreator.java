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

package com.google.connector.snowflakeToBQ.service.Instancecreator;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.migration.v2.MigrationServiceClient;
import com.google.cloud.bigquery.migration.v2.MigrationServiceSettings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import javax.annotation.PostConstruct;
import lombok.Getter;
import net.snowflake.client.jdbc.internal.apache.tika.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Class to create the instance of MigrationServiceClient using service account if provided else using application
 * default credentials.
 */
@Service
public class MigrationServiceInstanceCreator {

  private static final Logger log = LoggerFactory.getLogger(MigrationServiceInstanceCreator.class);

  @Value("${service.account.file.path}")
  private String serviceAccountFilePath;

  @Getter private MigrationServiceClient migrationServiceClient;

  @PostConstruct
  private void initStaticMigrationServiceInstance() {
    try {
      // Checking if service account path is blank in properties file, if black the service account
      // will not be used for authentication of GCP resources rather application default credentials
      // will be used.
      if (StringUtils.isEmpty(serviceAccountFilePath)) {
        migrationServiceClient = MigrationServiceClient.create();
        log.info(
            "Service account path is empty hence creating the MigrationService instance using"
                + " GOOGLE_APPLICATION_CREDENTIALS");
      } else {
        MigrationServiceSettings migrationServiceSettings =
            MigrationServiceSettings.newBuilder()
                .setCredentialsProvider(new MigrationCredentialProvider())
                .build();

        migrationServiceClient = MigrationServiceClient.create(migrationServiceSettings);
        log.info(
            "Service account path is given hence creating the MigrationService instance using"
                + " service account");
      }
    } catch (Exception e) {
      throw new RuntimeException("Unable to create migration service instance");
    }
  }

  /* Method to create {@link GoogleCredentials} using service account. */
  private GoogleCredentials getGoogleCredentialsUsingSA() {
    File credentialsPath = new File(serviceAccountFilePath);
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    } catch (IOException e) {
      log.error(e.getMessage());
      throw new RuntimeException(
          "Error while creating credential object from service account file");
    }
  }

  private class MigrationCredentialProvider implements CredentialsProvider {
    @Override
    public Credentials getCredentials() {
      return getGoogleCredentialsUsingSA();
    }
  }
}
