/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tsfile.encrypt;

import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.exception.encrypt.EncryptKeyLengthNotMatchException;
import org.apache.tsfile.file.metadata.enums.EncryptionType;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class AES128Encryptor implements IEncryptor {
  private final Cipher AES;

  private final SecretKeySpec secretKeySpec;

  private final IvParameterSpec ivParameterSpec;

  AES128Encryptor(byte[] key) {
    if (key.length != 16) {
      throw new EncryptKeyLengthNotMatchException(16, key.length);
    }
    secretKeySpec = new SecretKeySpec(key, "AES");
    // Create IV parameter
    ivParameterSpec = new IvParameterSpec(key);
    try {
      // Create Cipher instance and initialize it for encryption in CTR mode without padding
      this.AES = Cipher.getInstance("AES/CTR/NoPadding");
      AES.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivParameterSpec);
    } catch (InvalidAlgorithmParameterException
        | NoSuchPaddingException
        | NoSuchAlgorithmException
        | InvalidKeyException e) {
      throw new EncryptException("AES128Encryptor init failed ", e);
    }
  }

  @Override
  public byte[] encrypt(byte[] data) {
    try {
      System.out.println("AES128Encryptor encrypt length: " + data.length);
      byte[] result = AES.doFinal(data);
      AES.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivParameterSpec);
      return result;
      //      return AES.doFinal(data);
    } catch (IllegalBlockSizeException
        | BadPaddingException
        | InvalidKeyException
        | InvalidAlgorithmParameterException e) {
      throw new EncryptException("AES128Encryptor encrypt failed ", e);
    }
  }

  @Override
  public byte[] encrypt(byte[] data, int offset, int size) {
    System.out.println(
        "encrypt data.length: " + data.length + ", offset: " + offset + ", size: " + size);
    return encrypt(Arrays.copyOfRange(data, offset, offset + size));
    //    try {
    //      return AES.doFinal(data, offset, size);
    //    } catch (IllegalBlockSizeException | BadPaddingException e) {
    //      throw new EncryptException("AES128Encryptor encrypt failed ", e);
    //    }
  }

  @Override
  public EncryptionType getEncryptionType() {
    return EncryptionType.AES128;
  }
}
