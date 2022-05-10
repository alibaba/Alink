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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.catalog.datahub.common.util;

import org.apache.commons.lang3.StringUtils;

/**
 * A simple StringUtil for blink-connectors based on org.apache.commons.lang3.StringUtils.
 */
public class BlinkStringUtil {

	public static boolean isEmpty(String... strs) {
		for (String str : strs) {
			if (StringUtils.isEmpty(str)) {
				return true;
			}
		}
		return false;
	}

	public static boolean isNotEmpty(String... strs) {
		return !isEmpty(strs);
	}

	public static String[] splitPreserveAllTokens(String src, String delimiter) {
		if (src == null) {
			return null;
		}
		if (delimiter == null) {
			return new String[]{src};
		}
		if (delimiter.length() == 1) {
			return StringUtils.splitPreserveAllTokens(src, delimiter.charAt(0));
		} else {
			return StringUtils.splitPreserveAllTokens(src, delimiter);
		}
	}

	public static String[] split(String src, String delimiter) {
		return StringUtils.split(src, delimiter);
	}

	public static String join(String[] src) {
		return join(src, ",");
	}

	public static String join(String[] src, String delimiter) {
		return StringUtils.join(src, delimiter);
	}

	/**
	 * Checks whether the given string is null, empty, or contains only whitespace/delimiter character.
	 */
	public static boolean isBlank(String str, String delimiter) {
		if (str == null || str.length() == 0) {
			return true;
		}
		if (null == delimiter) {
			return StringUtils.isBlank(str);
		}
		if (delimiter.length() == 1) {
			char dChar = delimiter.charAt(0);
			final int len = str.length();
			for (int i = 0; i < len; i++) {
				if (!Character.isWhitespace(str.charAt(i)) && dChar != str.charAt(i)) {
					return false;
				}
			}
		} else {
			String[] array = StringUtils.split(str, delimiter);
			for (String s : array) {
				if (!StringUtils.isBlank(s)) {
					return false;
				}
			}
		}
		return true;
	}

	public static boolean isEmptyKey(Object key) {
		if (key == null) {
			return true;
		}
		String val = String.valueOf(key);
		if (StringUtils.isBlank(val)) {
			return true;
		}
		return false;
	}

	/**
	 * Return the first candidate which is not a blank string, otherwise return null.
	 * @param candidates
	 */
	public static String coalesce(String... candidates) {
		if (null != candidates && candidates.length > 0) {
			for (String c : candidates) {
				if (!StringUtils.isBlank(c)) {
					return c;
				}
			}
		}
		return null;
	}
}
