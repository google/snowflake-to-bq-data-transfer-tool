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

package com.google.connector.snowflakeToBQ.util.encryption;

import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Class to help in encrypting and decrypting the value */
@Component
public class EncryptValues {
  private static final Logger log = LoggerFactory.getLogger(EncryptValues.class);

  private String encryptionSecretKey;

  @Value("${SECRET_KEY}")
  public void setEncryptionSecretKey(String encryptionSecretKey) {
    this.encryptionSecretKey = encryptionSecretKey;
  }

  /**
   * Method to encrypt the value using AES encryption in ECB mode with PKCS5 padding. It takes an
   * unencrypted value, an encryption secret key, and produce a byte array containing the encrypted
   * data.
   *
   * @param unencryptedValue value to be encrypted
   * @return encrypted value in string format
   */
  public String encryptValue(String unencryptedValue) {
    try {
      checkSecretKey();
      checkNull(
          unencryptedValue, ErrorCode.ENCRYPTION_ERROR, "Received value for encryption is blank");
      // initializes a Cipher instance for AES encryption in ECB mode with PKCS5 padding
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      SecretKeySpec secretKey =
          new SecretKeySpec(encryptionSecretKey.getBytes(StandardCharsets.UTF_8), "AES");
      // initializes the Cipher instance for encryption mode (as specified by Cipher.ENCRYPT_MODE)
      // and associates it with the secret key (secretKey) for encryption.
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
      byte[] encryptedData = cipher.doFinal(unencryptedValue.getBytes());
      return Base64.getEncoder().encodeToString(encryptedData);
    } catch (Exception e) {
      log.error("Error while encrypting the received value, Error:{}", e.getMessage());
      throw new SnowflakeConnectorException(
          e.getMessage(), ErrorCode.ENCRYPTION_ERROR.getErrorCode());
    }
  }

  /**
   * Method to decrypt the value using AES encryption in ECB mode with PKCS5 padding. It takes an
   * encrypted value, an encryption secret key, and produce a byte array containing the decrypted
   * data.
   *
   * @param encryptedValue value to be decrypted
   * @return decrypted value in string format
   */
  public String decryptValue(String encryptedValue) {
    try {
      checkSecretKey();
      checkNull(
          encryptedValue, ErrorCode.DECRYPTION_ERROR, "Received value for decryption is blank");

      // initializes a Cipher instance for AES encryption in ECB mode with PKCS5 padding
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      SecretKeySpec secretKey =
          new SecretKeySpec(encryptionSecretKey.getBytes(StandardCharsets.UTF_8), "AES");
      cipher.init(Cipher.DECRYPT_MODE, secretKey);
      // Decrypting the value
      byte[] decryptedData = cipher.doFinal(Base64.getDecoder().decode(encryptedValue));
      return new String(decryptedData);
    } catch (Exception e) {
      log.error("Error while decrypting the received value, Error:{}", e.getMessage());
      throw new SnowflakeConnectorException(
          e.getMessage(), ErrorCode.DECRYPTION_ERROR.getErrorCode());
    }
  }

  private void checkSecretKey() {
    if (StringUtils.isBlank(encryptionSecretKey)) {
      throw new SnowflakeConnectorException(
          "Secret Key to encrypt/decrypt data is not given, please set it in environment"
              + " variable(SECRET_KEY)",
          0);
    }
  }

  private static void checkNull(String value, ErrorCode errorcode, String errorMessage) {
    if (StringUtils.isBlank(value)) {
      throw new SnowflakeConnectorException(
          String.format("%s, %s", errorcode.getMessage(), errorMessage), errorcode.getErrorCode());
    }
  }
}
