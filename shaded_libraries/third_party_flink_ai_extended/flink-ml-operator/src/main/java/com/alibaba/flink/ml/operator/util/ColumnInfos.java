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

import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.MLException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.CaseClassSerializer;
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Product;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class ColumnInfos implements Serializable {
	public static Logger LOG = LoggerFactory.getLogger(ColumnInfos.class);

	private List<String> colNames = new ArrayList<>();
	private List<TypeInformation> tiInfos = new ArrayList<>();
	private TypeInformation originalTI = null;
	private boolean decomposed = false;

	public int count() {
		return colNames.size();
	}

	public String getColName(int i) {
		return colNames.get(i);
	}

	public TypeInformation getTiInfo(int i) {
		return tiInfos.get(i);
	}

	public DataTypes getDataTypes(int i) {
		TypeInformation ti = getTiInfo(i);
		Class<?> clazz = ti.getTypeClass();
		return DataTypeConversion.fromJavaClass(clazz);
	}

	public static ColumnInfos fromTypeInformation(TypeInformation ti) {
		ColumnInfos info = new ColumnInfos();
		info.originalTI = ti;
		//current only support these 4 types decomposition
		if (ti instanceof CaseClassTypeInfo
				|| ti instanceof TupleTypeInfo
				|| ti instanceof PojoTypeInfo
				|| ti instanceof RowTypeInfo) {
			CompositeType ct = (CompositeType) ti;
			String[] fieldNames = ct.getFieldNames();
			for (int i = 0; i < fieldNames.length; i++) {
				info.colNames.add(fieldNames[i]);
				info.tiInfos.add(ct.getTypeAt(i));
			}

			info.decomposed = true;
		} else {
			info.colNames.add(MLConstants.INTUT_DEFAULT_NAME);
			info.tiInfos.add(ti);
		}

		info.checkColumns();

		return info;
	}

	public static ColumnInfos dummy() {
		return new ColumnInfos();
	}

	public Object getField(Object obj, int index) {
		if (decomposed) {
			if (originalTI instanceof PojoTypeInfo) {
				PojoTypeInfo pti = (PojoTypeInfo) originalTI;
				PojoField field = pti.getPojoFieldAt(index);
				try {
					return field.getField().get(obj);
				} catch (IllegalAccessException e) {
					LOG.error("Fail to get field " + field.toString(), e);
				}
			} else if (originalTI instanceof CaseClassTypeInfo) {
				CaseClassTypeInfo cti = (CaseClassTypeInfo) originalTI;
				Product pt = (Product) obj;
				return pt.productElement(index);
			} else if (originalTI instanceof TupleTypeInfo) {
				Tuple tuple = (Tuple) obj;
				return tuple.getField(index);
			} else if (originalTI instanceof RowTypeInfo) {
				Row row = (Row) obj;
				return row.getField(index);
			}

		} else {
			Object[] row = (Object[]) obj;
			return row[index];
		}
		return obj;
	}

	public Object createResultObject(List<Object> fields, ExecutionConfig config) throws MLException {
		if (fields.size() != count()) {
			throw new MLException("Invalid field number for create object for class "
					+ originalTI.getTypeClass() + ". Needs " + count() + " fields, while having " + fields
					.size() + " fields.");
		}
		if (decomposed) {
			if (originalTI instanceof PojoTypeInfo) {
				PojoTypeInfo pto = (PojoTypeInfo) originalTI;
				try {
					Object result = originalTI.getTypeClass().newInstance();
					for (int i = 0; i < count(); i++) {
						PojoField field = pto.getPojoFieldAt(i);
						field.getField().set(result, fields.get(i));
					}
					return result;
				} catch (Exception e) {
					throw new MLException("Fail to initiate POJO object. The type is " + originalTI.getTypeClass(), e);
				}
			} else if (originalTI instanceof CaseClassTypeInfo) {
				CaseClassSerializer ts = (CaseClassSerializer) originalTI.createSerializer(config);
				return ts.createInstance(fields.toArray());
			} else if (originalTI instanceof TupleTypeInfo) {
				try {
					Tuple tuple = Tuple.getTupleClass(count()).newInstance();
					for (int i = 0; i < count(); i++) {
						tuple.setField(fields.get(i), i);
					}
					return tuple;
				} catch (IllegalAccessException | InstantiationException e) {
					throw new MLException("Failed to create Tuple object for type " +
							originalTI.getTypeClass().getCanonicalName(), e);
				}
			} else if (originalTI instanceof RowTypeInfo) {
				Row row = new Row(count());
				for (int i = 0; i < count(); i++) {
					row.setField(i, fields.get(i));
				}
				return row;
			}
		}
		return fields.get(0);
	}

	private void checkColumns() {
		//check whether has duplicate column names
		Set<String> uniqueItems = new HashSet<>();
		List<?> duplicates = colNames.stream().filter(o -> !uniqueItems.add(o)).collect(Collectors.toList());
		if (!duplicates.isEmpty()) {
			throw new IllegalArgumentException("Found duplicated column name(s): " + Joiner.on(", ").join(duplicates));
		}

		//check the types are simple types supported
		for (TypeInformation ti : tiInfos) {
			Class<?> clazz = ti.getTypeClass();
			Preconditions.checkArgument(DataTypeConversion.fromJavaClass(clazz) != null,
					"Data type " + clazz.getName() + " CAN NOT convert to a Tensorflow data type.");
		}
	}

	public TypeInformation getOriginalTI() {
		return originalTI;
	}

	public boolean isDecomposed() {
		return decomposed;
	}

	public Map<String, String> getNameToTypeMap() {
		Map<String, String> res = new HashMap<>();
		for (int i = 0; i < colNames.size(); i++) {
			res.put(getColName(i), getDataTypes(i).toString());
		}
		return res;
	}
}
