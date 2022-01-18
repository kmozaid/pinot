/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.utils;

import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.StringUtils;

/**
 * AES Encryption Algorithm
 * @author Kevin Xu
 *
 */
public class EncryptUtils {

    /**
     * encryption and decryption keys, even external
     */
    public static final String AES_DATA_SECURITY_KEY = "4%YkW!@g5LGcf9Ut";
    /**
     * Algorithm/Encryption Mode/Padding
     */
    private static final String AES_PKCS5P = "AES/ECB/PKCS5Padding";

    private static final String AES_PERSON_KEY_SECURITY_KEY = "pisnyMyZYXuCNcRd";

    private EncryptUtils() {

    }

    /**
     * Encryption
     *
     * @param str
     *            String to be encrypted
     * @param key
     *            key
     * @return
     * @throws Exception
     */
    public static String encrypt(String str, String key) {
        if (StringUtils.isEmpty(key)) {
            throw new RuntimeException("key shouldn't be null");
        }
        try {
            if (str == null) {
                return null;
            }
            // Determine whether the Key is 16 bits
            if (key.length() != 16) {
                return null;
            }
            byte[] raw = key.getBytes("UTF-8");
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
            // Algorithm/Mode/Complement Method
            Cipher cipher = Cipher.getInstance(AES_PKCS5P);
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            byte[] encrypted = cipher.doFinal(str.getBytes("UTF-8"));
            // Here, BASE64 is used for the transcoding function, and it can also play the role of 2 encryptions.

            Base64.Encoder encoder = Base64.getEncoder();
            return encoder.encodeToString(encrypted);
//            return new BASE64Encoder().encode(encrypted);
        } catch (Exception ex) {
            return null;
        }

    }

    /**
     * decryption
     *
     * @param str String to decrypt
     * @param key key
     * @return
     */
    public static String decrypt(String str, String key) {
        if (StringUtils.isEmpty(key)) {
            throw new RuntimeException("key shouldn't be null");
        }
        try {
            if (str == null) {
                return null;
            }
            // Determine whether the Key is 16 bits
            if (key.length() != 16) {
                return null;
            }
            byte[] raw = key.getBytes("UTF-8");
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
            Cipher cipher = Cipher.getInstance(AES_PKCS5P);
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
            // Decrypt with base64 first
            Base64.Decoder decoder = Base64.getDecoder();
            byte[] encrypted = decoder.decode(str);
//            byte[] encrypted = new BASE64Decoder().decodeBuffer(str);
            try {
                byte[] original = cipher.doFinal(encrypted);
                String originalString = new String(original, "UTF-8");
                return originalString;
            } catch (Exception e) {
                return null;
            }
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Encryption
     *
     * @param str String to be encrypted
     * @return
     * @throws Exception
     */
    public static String encrypt(String str) {
        return encrypt(str, AES_DATA_SECURITY_KEY);
    }

    /**
     * Decryption
     * @param str String to decrypt
     * @return
     */
    public static String decrypt(String str) {
        return decrypt(str, AES_DATA_SECURITY_KEY);
    }

    /**
     * Decrypt certain fields when querying
     *
     * @param str
     * @return
     */
    public static String aesDecrypt(String str) {
        if (StringUtils.isBlank(str)) {
            return " ";
        }
        String sql = " AES_DECRYPT(from_base64(" + str + ")," + "'" + AES_DATA_SECURITY_KEY + "')";
        return sql;
    }

    /**
     * Encrypt personKey
     *
     * @param personKey
     * @return
     */
    public static String encryptPersonKey(String personKey) {
        return EncryptUtils.encrypt(personKey, AES_PERSON_KEY_SECURITY_KEY);
    }

    /**
     * Decrypt personKey
     *
     * @param personKey
     * @return
     */
    public static String decryptPersonKey(String personKey) {
        return EncryptUtils.decrypt(personKey, AES_PERSON_KEY_SECURITY_KEY);
    }

    public static void main(String[] args) {
//        AESEncryptUtils aesEncryptUtils = new AESEncryptUtils();
        String encrpyed = EncryptUtils.encrypt("Pda@wbxcalld2dd1dqdsdq211><??ing5", "G-KaPdSgVkYp3s6v");
        System.out.println(encrpyed);
        String restorePassword = EncryptUtils.decrypt(encrpyed, "G-KaPdSgVkYp3s6v");
        System.out.println(restorePassword);
    }

}