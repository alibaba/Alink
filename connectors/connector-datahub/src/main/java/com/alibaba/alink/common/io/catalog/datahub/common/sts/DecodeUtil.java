/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.catalog.datahub.common.sts;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DecodeUtil.
 */
public class DecodeUtil {
	private static Logger logger = LoggerFactory.getLogger(DecodeUtil.class);

	private static final String ENCODING = "UTF-8";

	private static final String CIPHER_ALGORITHM_ECB = "AES/ECB/PKCS5Padding";

	public static String decrypt(String strToDecrypt, String secret) throws Exception {
		if (strToDecrypt == null) {
			return null;
		}
		try {
			Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM_ECB);
			final SecretKeySpec secretKey = new SecretKeySpec(secret.getBytes(), "AES");
			cipher.init(Cipher.DECRYPT_MODE, secretKey);
			String decryptedString = new String(cipher.doFinal(Base64.decodeBase64(strToDecrypt)));
			return decryptedString;
		} catch (Exception e) {
			logger.error("decode " + strToDecrypt + " failed " + e.getStackTrace());
			throw e;
		}
	}
}
