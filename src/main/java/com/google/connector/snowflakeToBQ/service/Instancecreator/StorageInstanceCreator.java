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

package com.google.connector.snowflakeToBQ.service.Instancecreator;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import javax.annotation.PostConstruct;
import lombok.Getter;
import net.snowflake.client.jdbc.internal.apache.tika.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/**
 * Class to create the instance of Google cloud storage using service account if provided else using
 * application default credentials.
 */
@Service
public class StorageInstanceCreator {

  private static final Logger log = LoggerFactory.getLogger(StorageInstanceCreator.class);

  @Value("${service.account.file.path}")
  private String serviceAccountFilePath;

  @Getter private Storage storageClient;

  @PostConstruct
  private void initStaticStorageInstance() {

    try {
      // Checking if service account path is blank in properties file, if black the service account
      // will not be used for authentication of GCP resources rather application default credentials
      // will be used.
      if (StringUtils.isEmpty(serviceAccountFilePath)) {
        storageClient = StorageOptions.getDefaultInstance().getService();
        log.info(
            "Service account path is empty hence creating the Storage instance using"
                + " GOOGLE_APPLICATION_CREDENTIALS");
      } else {
        GoogleCredentials credentials = getGoogleCredentialsUsingSA();
        storageClient = getStorageClient(credentials);
        log.info(
            "Service account path is given hence creating the Storage instance using service"
                + " account");
      }
    } catch (Exception e) {
      throw new RuntimeException("Unable to create Storage instance");
    }
  }

  private Storage getStorageClient(GoogleCredentials credentials) {
    log.info("Inside getStorageClient() to create the storage client");
    return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
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
}
