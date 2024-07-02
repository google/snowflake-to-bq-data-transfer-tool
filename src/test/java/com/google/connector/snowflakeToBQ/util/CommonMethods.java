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

package com.google.connector.snowflakeToBQ.util;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class CommonMethods {

  public static SecretKey generateSecretKey() {
    KeyGenerator keyGenerator;
    try {
      keyGenerator = KeyGenerator.getInstance("AES");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    keyGenerator.init(256);
    return keyGenerator.generateKey();
  }

  public static byte[] generateInitializationVector() {
    SecureRandom secureRandom = new SecureRandom();
    // 16 bytes (128 bits) for AES IV
    byte[] ivBytes = new byte[16];
    secureRandom.nextBytes(ivBytes);
    return ivBytes;
  }
}
