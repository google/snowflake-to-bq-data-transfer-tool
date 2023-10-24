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
import com.google.connector.snowflakeToBQ.model.EncryptedData;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Class to help in encrypting and decrypting the value */
@Component
public class EncryptValues {
  private static final Logger log = LoggerFactory.getLogger(EncryptValues.class);

  /**
   * Method to encrypt the value using AES encryption in CBC mode with PKCS5 padding. It takes an
   * unencrypted value, an encryption secret key, IV Parameter and produce a byte array containing
   * the encrypted data.
   *
   * @param unencryptedValue value to be encrypted
   * @return encrypted value in string format
   */
  public EncryptedData encryptValue(String unencryptedValue) {
    try {
      checkNull(
          unencryptedValue, ErrorCode.ENCRYPTION_ERROR, "Received value for encryption is blank");

      // initializes a Cipher instance for AES encryption in CBC mode with PKCS5 padding
      Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
      byte[] ivBytes = generateInitializationVector();
      IvParameterSpec ivParameterSpec = new IvParameterSpec(ivBytes);
      SecretKey secretKey = generateSecretKey();
      // initializes the Cipher instance for encryption mode (as specified by Cipher.ENCRYPT_MODE)
      // and associates it with the secret key (secretKey) and IVParameter for encryption.
      cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivParameterSpec);

      byte[] encryptedData = cipher.doFinal(unencryptedValue.getBytes(StandardCharsets.UTF_8));

      // Combine IV and ciphertext, and encode as Base64
      byte[] combined = new byte[ivBytes.length + encryptedData.length];
      System.arraycopy(ivBytes, 0, combined, 0, ivBytes.length);
      System.arraycopy(encryptedData, 0, combined, ivBytes.length, encryptedData.length);

      return new EncryptedData(
          Base64.getEncoder().encodeToString(combined), secretKey, ivParameterSpec.getIV());
    } catch (Exception e) {
      log.error("Error while encrypting the received value, Error:{}", e.getMessage());
      throw new SnowflakeConnectorException(
          e.getMessage(), ErrorCode.ENCRYPTION_ERROR.getErrorCode());
    }
  }

  /**
   * Method to decrypt the value using AES encryption in CBC mode with PKCS5 padding. It takes an
   * encrypted value, an encryption secret key, IV Parameter and produce a byte array containing the
   * decrypted data.
   *
   * @param encryptedValue value to be decrypted
   * @return decrypted value in string format
   */
  public String decryptValue(EncryptedData encryptedValue) {
    try {
      if (encryptedValue == null) {
        throw new SnowflakeConnectorException(
            String.format(
                "%s, %s",
                ErrorCode.DECRYPTION_ERROR.getMessage(), "Received value for decryption is blank"),
            ErrorCode.DECRYPTION_ERROR.getErrorCode());
      }
      byte[] ivBytes = encryptedValue.getInitializationVector();

      byte[] combined = Base64.getDecoder().decode(encryptedValue.getCiphertext());

      // initializes a Cipher instance for AES encryption in CBC mode with PKCS5 padding
      Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

      IvParameterSpec ivParameterSpec = new IvParameterSpec(ivBytes);
      cipher.init(Cipher.DECRYPT_MODE, encryptedValue.getSecretKey(), ivParameterSpec);
      // Decrypting the value
      byte[] decryptedData =
          cipher.doFinal(combined, ivBytes.length, combined.length - ivBytes.length);

      return new String(decryptedData, StandardCharsets.UTF_8);
    } catch (Exception e) {
      log.error("Error while decrypting the received value, Error:{}", e.getMessage());
      throw new SnowflakeConnectorException(
          e.getMessage(), ErrorCode.DECRYPTION_ERROR.getErrorCode());
    }
  }

  private void checkNull(String value, ErrorCode errorcode, String errorMessage) {
    if (StringUtils.isBlank(value)) {
      throw new SnowflakeConnectorException(
          String.format("%s, %s", errorcode.getMessage(), errorMessage), errorcode.getErrorCode());
    }
  }

  private SecretKey generateSecretKey() throws NoSuchAlgorithmException {
    KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256);
    return keyGenerator.generateKey();
  }

  private byte[] generateInitializationVector() {
    SecureRandom secureRandom = new SecureRandom();
    // 16 bytes (128 bits) for AES IV
    byte[] ivBytes = new byte[16];
    secureRandom.nextBytes(ivBytes);
    return ivBytes;
  }
}
