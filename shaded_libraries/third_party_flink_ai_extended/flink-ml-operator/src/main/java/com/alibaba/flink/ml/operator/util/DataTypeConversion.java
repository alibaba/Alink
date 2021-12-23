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

package com.alibaba.flink.ml.operator.util;

import java.util.HashMap;
import java.util.Map;

/**
 * convert java class to DataTypes.
 */
public class DataTypeConversion {
	/**
	 * @param c java class.
	 * @return java class corresponds to DataTypes.
	 */
	public static DataTypes fromJavaClass(Class<?> c) {
		DataTypes dt = builtinMaps.get(c);
		if (dt != null) {
			return dt;
		}
		//whether c implements java.lang.CharSequence
		if (CharSequence.class.isAssignableFrom(c)) {
			return DataTypes.STRING;
		}

		return null;
	}

	private static Map<Class<?>, DataTypes> builtinMaps = new HashMap<Class<?>, DataTypes>() {{
		put(Float.class, DataTypes.FLOAT_32);
		put(Double.class, DataTypes.FLOAT_64);
		put(Long.class, DataTypes.INT_64);
		put(Integer.class, DataTypes.INT_32);
		put(Character.class, DataTypes.UINT_16);
		put(Short.class, DataTypes.INT_16);
		put(Byte.class, DataTypes.INT_8);
		put(Boolean.class, DataTypes.BOOL);
		put(String.class, DataTypes.STRING);
		put(byte[].class, DataTypes.STRING);
		put(float[].class, DataTypes.FLOAT_32_ARRAY);
	}};
}
