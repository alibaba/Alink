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

package com.alibaba.alink.common.dl.coding;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.dl.data.DataTypesV2;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.flink.ml.coding.CodingException;
import com.alibaba.flink.ml.tensorflow2.coding.ExampleCodingConfig;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Product;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ExampleCoding's configuration. support array,pojo,scala case class, flink row, tuple.
 * <p>
 * Modified to adopt {@link DataTypesV2} to support more array types.
 */
public class ExampleCodingConfigV2 implements Serializable {
	public static Logger LOG = LoggerFactory.getLogger(ExampleCodingConfigV2.class);

	public enum ObjectType {
		ARRAY,
		POJO,
		CASE_CLASS,
		ROW,
		TUPLE
	}

	private List <String> names = new ArrayList <>();
	private List <DataTypesV2> types = new ArrayList <>();
	private ObjectType objectType = ObjectType.ARRAY;
	private Class objectClass;

	public int count() {
		return names.size();
	}

	public String getColName(int i) {
		return names.get(i);
	}

	public DataTypesV2 getType(int i) {
		return types.get(i);
	}

	/**
	 * create ExampleCodingConfig json string.
	 *
	 * @param names       example feature names.
	 * @param types       example feature data types.
	 * @param objectType  ObjectType.
	 * @param objectClass java object class.
	 * @return configuration json string.
	 */
	public static String createExampleConfigStr(String[] names, DataTypesV2[] types,
												ObjectType objectType, Class objectClass) {
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("objectClass", objectClass.getCanonicalName());
		jsonObject.put("objectType", objectType.name());
		JSONArray n = new JSONArray();
		n.addAll(Arrays.asList(names));
		JSONArray t = new JSONArray();
		t.addAll(Arrays.asList(types));
		jsonObject.put("names", n);
		jsonObject.put("types", t);
		return jsonObject.toJSONString();
	}

	/**
	 * create ExampleCodingConfig from json.
	 *
	 * @param jsonObject configuration
	 * @return ExampleCodingConfig type config.
	 * @throws CodingException
	 */
	public ExampleCodingConfig fromJsonObject(JSONObject jsonObject) throws CodingException {
		ExampleCodingConfig config = new ExampleCodingConfig();
		JSONArray names = jsonObject.getJSONArray("names");
		JSONArray types = jsonObject.getJSONArray("types");
		this.names = names.toJavaList(String.class);
		this.types = types.toJavaList(DataTypesV2.class);
		objectType = ObjectType.valueOf(jsonObject.getString("objectType"));
		try {
			objectClass = Class.forName(jsonObject.getString("objectClass"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new CodingException(e.getMessage());
		}
		checkColumns();
		return config;
	}

	public ExampleCodingConfigV2() {
	}

	/**
	 * get feature by index.
	 *
	 * @param obj   example corresponds to java object.
	 * @param index feature index.
	 * @return feature.
	 * @throws CodingException
	 */
	public Object getField(Object obj, int index) throws CodingException {
		switch (objectType) {
			case ROW:
				Row row = (Row) obj;
				return row.getField(index);
			case POJO:
				try {
					return obj.getClass().getField(names.get(index)).get(obj);
				} catch (IllegalAccessException | NoSuchFieldException e) {
					e.printStackTrace();
					throw new CodingException(e.getMessage());
				}
			case TUPLE:
				Tuple tuple = (Tuple) obj;
				return tuple.getField(index);
			case CASE_CLASS:
				Product pt = (Product) obj;
				return pt.productElement(index);
			case ARRAY:
				Object[] array = (Object[]) obj;
				return array[index];
			default:
				return obj;
		}
	}

	/**
	 * create example corresponds to java object by given list of object.
	 *
	 * @param fields example Feature corresponds to java object.
	 * @return example corresponds to java object.
	 * @throws CodingException
	 */
	public Object createResultObject(List <Object> fields) throws CodingException {
		if (fields.size() != count()) {
			throw new CodingException("Invalid field number for create object "
				+ ". Needs " + count() + " fields, while having " + fields.size() + " fields.");
		}
		switch (objectType) {
			case ARRAY:
				Object[] o = new Object[count()];
				for (int i = 0; i < count(); i++) {
					o[i] = fields.get(i);
				}
				return o;
			case TUPLE:
				try {
					Tuple tuple = Tuple.getTupleClass(count()).newInstance();
					for (int i = 0; i < count(); i++) {
						tuple.setField(fields.get(i), i);
					}
					return tuple;
				} catch (IllegalAccessException | InstantiationException e) {
					throw new CodingException("Failed to create Tuple object for type ", e);
				}
			case CASE_CLASS:
			case POJO:
				try {
					Object object = objectClass.newInstance();
					for (int i = 0; i < count(); i++) {
						objectClass.getField(names.get(i)).set(object, fields.get(i));
					}
				} catch (InstantiationException | IllegalAccessException | NoSuchFieldException e) {
					e.printStackTrace();
					throw new CodingException(e.getMessage());
				}
			case ROW:
				Row row = new Row(count());
				for (int i = 0; i < count(); i++) {
					row.setField(i, fields.get(i));
				}
				return row;
		}
		throw new CodingException("unsupport object type:" + objectType);

	}

	private void checkColumns() {
		//check whether has duplicate column names
		Set <String> uniqueItems = new HashSet <>();
		List <?> duplicates = names.stream().filter(o -> !uniqueItems.add(o)).collect(Collectors.toList());
		if (!duplicates.isEmpty()) {
			throw new AkIllegalArgumentException("Found duplicated column name(s): " + Joiner.on(", ").join(duplicates));
		}
	}
}
