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

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.util.ErrorCode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class EncryptionValuesTest extends AbstractTestBase {
  private static final Logger log = LoggerFactory.getLogger(EncryptionValuesTest.class);

  @Autowired private EncryptValues encryptValues;

  @Spy private EncryptValues encryptValues1;

  @Test()
  public void testStringEncrypt() {
    String dummyValue = "This is the test string to be encrypted for test";
    encryptValues.setEncryptionSecretKey("test1234testterststsssssssssdsdc");
    String encryptedValue = encryptValues.encryptValue(dummyValue);
    String decryptedValue = encryptValues.decryptValue(encryptedValue);
    Assert.assertEquals(dummyValue, decryptedValue);
  }

  @Test()
  public void testStringEncryptShortSecretKey() {
    // key less than 32 byte
    encryptValues.setEncryptionSecretKey("shortkey");
    String dummyValue = "This is the test string to be encrypted for test";
    try {
      String encryptedValue = encryptValues.encryptValue(dummyValue);
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals("Invalid AES key length: 8 bytes", e.getMessage());
    }
  }

  @Test()
  public void testStringEncryptException() {
    try {
      encryptValues.setEncryptionSecretKey("testkeytestkeytestkeytestkey");
      encryptValues.encryptValue(null);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(
          "Error: Encrypting the values, Received value for encryption is blank", e.getMessage());
      Assert.assertEquals(ErrorCode.ENCRYPTION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test()
  public void testStringDecryptException() {
    try {
      encryptValues.setEncryptionSecretKey("testkey");
      encryptValues.decryptValue(null);
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(
          "Error: Decrypting the values, Received value for decryption is blank", e.getMessage());
      Assert.assertEquals(ErrorCode.DECRYPTION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test()
  public void testStringEncryptSecretKeyNull() {
    try {
      encryptValues.setEncryptionSecretKey(null);
      encryptValues.encryptValue("test");
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(
          "Secret Key to encrypt/decrypt data is not given, please set it in environment"
              + " variable(SECRET_KEY)",
          e.getMessage());
      Assert.assertEquals(ErrorCode.ENCRYPTION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test()
  public void testStringDecryptSecretKeyNull() {
    try {
      encryptValues.setEncryptionSecretKey(null);
      encryptValues.decryptValue("test");
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(
          "Secret Key to encrypt/decrypt data is not given, please set it in environment"
              + " variable(SECRET_KEY)",
          e.getMessage());
      Assert.assertEquals(ErrorCode.DECRYPTION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test()
  public void testStringDecryptSecretKeyEmpty() {
    try {
      encryptValues.setEncryptionSecretKey("");
      encryptValues.decryptValue("test");
      Assert.fail();
    } catch (SnowflakeConnectorException e) {
      Assert.assertEquals(
          "Secret Key to encrypt/decrypt data is not given, please set it in environment"
              + " variable(SECRET_KEY)",
          e.getMessage());
      Assert.assertEquals(ErrorCode.DECRYPTION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }
}
