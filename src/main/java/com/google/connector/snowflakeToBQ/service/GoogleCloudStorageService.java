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

package com.google.connector.snowflakeToBQ.service;

import com.google.cloud.storage.*;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.GCSDetailsDataDTO;
import com.google.connector.snowflakeToBQ.service.Instancecreator.StorageInstanceCreator;
import com.google.connector.snowflakeToBQ.util.PropertyManager;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Class to perform all the operations related to Google cloud Storage. */
@Service
public class GoogleCloudStorageService {
  private static final Logger log = LoggerFactory.getLogger(GoogleCloudStorageService.class);

  final StorageInstanceCreator storageInstanceCreator;
  final ApplicationConfigDataService applicationConfigDataService;

  public GoogleCloudStorageService(
      StorageInstanceCreator storageInstanceCreator,
      ApplicationConfigDataService applicationConfigDataService) {
    this.storageInstanceCreator = storageInstanceCreator;
    this.applicationConfigDataService = applicationConfigDataService;
  }

  /**
   * Method to write the ddls into the given GCS location as a file.
   *
   * @param ddlMap map containing table name and respective ddls for it.
   * @param gcsDetailsDataDTO receive request data which contains information required to perform
   *     this method.
   * @return @{@link List} of {@link GCSDetailsDataDTO} which contains the information related to
   *     GCS path specifically the path of the DDLs files created as part of this method's
   *     execution.
   */
  public List<GCSDetailsDataDTO> writeToGCS(
      Map<String, String> ddlMap, GCSDetailsDataDTO gcsDetailsDataDTO) {
    moveFolder(gcsDetailsDataDTO.getSourceDatabaseName(), gcsDetailsDataDTO.getGcsBucketForDDLs());
    List<GCSDetailsDataDTO> gcsDetailsDataDTOS = new ArrayList<>();
    ddlMap.forEach(
        (tableName, ddl) -> {
          // Creating the full gcs path. Path will be like
          // bucket_name/database_name/schema_name/table_name.sql
          BlobInfo blobInfo =
              BlobInfo.newBuilder(
                      gcsDetailsDataDTO.getGcsBucketForDDLs(),
                      String.format(
                          "%s/%s/%s/%s.sql",
                          PropertyManager.DDL_PREFIX,
                          gcsDetailsDataDTO.getSourceDatabaseName(),
                          gcsDetailsDataDTO.getSourceSchemaName(),
                          tableName))
                  .build();
          // Replacing the table name in the ddl in bigquery format. Received ddls only contains
          // tablename, including these details helps during translation. By providing translation
          // mapping these values will get replaced with BigQuery specific value
          ddl =
              ddl.replace(
                  tableName,
                  String.format(
                      "%s.%s.%s",
                      gcsDetailsDataDTO.getSourceDatabaseName(),
                      gcsDetailsDataDTO.getSourceSchemaName(),
                      tableName));

          // write the file in GCS
          Blob blob =
              storageInstanceCreator
                  .getStorageClient()
                  .create(blobInfo, ddl.getBytes(StandardCharsets.UTF_8));

          // Cloning the incoming object as it contains several values needed in the new object,
          // along with additional values. We generate multiple objects from a single one because
          // the incoming object specifies the input GCS path where all the output objects will be
          // written. This method creates DDLs files in GCS (tables) based on the received DDLs and
          // table name. All these files (tables) require processing and should be stored in the
          // Application table as per design, hence list of GCSDetailsDataDTO get returned from this
          // method.
          GCSDetailsDataDTO gcsDetailsDataDTOCloned = SerializationUtils.clone(gcsDetailsDataDTO);
          gcsDetailsDataDTOCloned.setSourceDatabaseName(gcsDetailsDataDTO.getSourceDatabaseName());
          gcsDetailsDataDTOCloned.setSourceSchemaName(gcsDetailsDataDTO.getSourceSchemaName());
          gcsDetailsDataDTOCloned.setTargetDatabaseName(gcsDetailsDataDTO.getTargetDatabaseName());
          gcsDetailsDataDTOCloned.setTargetSchemaName(gcsDetailsDataDTO.getTargetSchemaName());
          // Updating path of the Snowflake DDLs file after saving it GCS
          gcsDetailsDataDTOCloned.setSnowflakeDDLsPath(
              String.format("gs://%s/%s", blob.getBucket(), blob.getName()));
          gcsDetailsDataDTOCloned.setSourceTableName(tableName);
          // Setting this property to true make sure that in table this step is completed.
          gcsDetailsDataDTOCloned.setSourceDDLCopied(true);
          gcsDetailsDataDTOS.add(gcsDetailsDataDTOCloned);
          log.info("File written to GCS {}::", blob.getMediaLink());
        });
    return gcsDetailsDataDTOS;
  }

  public boolean moveFolder(String databaseName, String translationGcsFolder) {
    boolean moveFolderStatus;
    // Check if any object exists with the specified prefix (folder path)
    // 2023_06_03_12_29_03/
    try {
      String destinationFolderName =
          PropertyManager.getDateInDesiredFormat(
              LocalDateTime.now(), PropertyManager.OUTPUT_FORMATTER);
      // DATA_FOR_CODE_TEST
      storageInstanceCreator
          .getStorageClient()
          .list(translationGcsFolder, Storage.BlobListOption.prefix(PropertyManager.DDL_PREFIX))
          .iterateAll()
          .forEach(
              blob -> {
                // Construct the destination object name
                String destinationObjectPath =
                    // e.g backup-ddls
                    PropertyManager.BACKUP_FOLDER_PREFIX
                        + "/"
                        // 2023_07_22_17_18_59(Date formed)
                        + destinationFolderName
                        // blog.getName()=snowflake-ddls/TEST_DATABASE/PUBLIC/DATES_VALUE.sql,
                        // databaseName=TEST_DATABASE
                        // So execution of below string will give
                        // "/TEST_DATABASE/PUBLIC/DATES_VALUE.sql, datbaseName=TEST_DATABASE" as in
                        // substring databasename will be the beginning index
                        + blob.getName().substring(databaseName.length() + 1);
                // DATA_FOR_CODE_TEST/ALL_DATATYPE.sql
                BlobId sourceObject = BlobId.of(translationGcsFolder, blob.getName());
                // 2023_06_03_12_29_03/ALL_DATATYPE.sql
                BlobId targetObject = BlobId.of(translationGcsFolder, destinationObjectPath);
                Storage.CopyRequest request =
                    Storage.CopyRequest.newBuilder()
                        .setSource(sourceObject)
                        .setTarget(targetObject)
                        .build();
                // Copy the object to the destination folder
                CopyWriter copyWriter = storageInstanceCreator.getStorageClient().copy(request);

                // Wait for the copy operation to complete
                copyWriter.getResult();
                storageInstanceCreator
                    .getStorageClient()
                    .delete(BlobId.of(translationGcsFolder, blob.getName()));
              });
      // snowflake-to-gcs-copy-into-may/DATA_FOR_CODE_TEST
      boolean deleteStatus =
          storageInstanceCreator
              .getStorageClient()
              .delete(translationGcsFolder, databaseName + "/");
      log.info("Root folder of the bucket path::{}, deleted::{}", databaseName, deleteStatus);
      moveFolderStatus = true;
      System.out.println("Folder copied successfully.");
    } catch (Exception e) {
      throw new SnowflakeConnectorException(e.getMessage(), 0);
    }
    return moveFolderStatus;
  }

  /**
   * Method to get the content of the file present in GCS
   *
   * @param bucketName Name of the bucket containing the file
   * @param filePath path of the file basically in GCS.
   * @return Content of the file as String
   */
  public String getContentFromGCSFile(String bucketName, String filePath) {
    if (StringUtils.isBlank(bucketName) || StringUtils.isBlank(filePath)) {
      String errorMessage = "bucket name or filepath is blank";
      log.error(errorMessage);
      throw new SnowflakeConnectorException(errorMessage, 0);
    }
    byte[] ddlBytes = storageInstanceCreator.getStorageClient().readAllBytes(bucketName, filePath);
    return new String(ddlBytes, StandardCharsets.UTF_8);
  }

  private BlobInfo createFolder(String bucketName, String folderName) {
    BlobInfo folderInfo = BlobInfo.newBuilder(BlobId.of(bucketName, folderName + "/")).build();
    return storageInstanceCreator.getStorageClient().create(folderInfo);
  }
}
