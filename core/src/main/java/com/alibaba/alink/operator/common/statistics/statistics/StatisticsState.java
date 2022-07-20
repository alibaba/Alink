/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.operator.common.statistics.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.AlinkTypes;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;

public class StatisticsState implements Serializable {
	private static final long serialVersionUID = -8289262092731078030L;

	/**
	 * Checkpoint state of {@link SummaryResultTable}.
	 */
	public static class SrtState implements Serializable {
		private static final long serialVersionUID = 6401346389476442953L;
		public String[] colNames;
		public String[] colClassNames;
		public double[][] dotProduction;

		public List <SrcState> stringSrcStates;
		public List <SrcState> booleanSrcStates;
		public List <SrcState> integerSrcStates;
		public List <SrcState> longSrcStates;
		public List <SrcState> floatSrcStates;
		public List <SrcState> doubleSrcStates;
		public List <SrcState> timestampSrcStates;

		public SrtState() { // to make it POJO
		}

		//colClasses is stat col types.
		public SrtState(Class[] colClasses, SummaryResultTable srt) {
			this.colClassNames = new String[colClasses.length];
			for (int i = 0; i < colClasses.length; i++) {
				this.colClassNames[i] = colClasses[i].getName();
			}

			this.colNames = srt.colNames;
			this.dotProduction = srt.dotProduction;
			assert colClasses.length == srt.src.length;

			for (int i = 0; i < srt.src.length; i++) {
				if (colClassNames[i].equals(String.class.getName())) {
					if (stringSrcStates == null) {
						stringSrcStates = new ArrayList <>();
					}
					stringSrcStates.add(new SrcState(srt.src[i]));
				} else if (colClassNames[i].equals(Boolean.class.getName())) {
					if (booleanSrcStates == null) {
						booleanSrcStates = new ArrayList <>();
					}
					booleanSrcStates.add(new SrcState(srt.src[i]));
				} else if (colClassNames[i].equals(Integer.class.getName())) {
					if (integerSrcStates == null) {
						integerSrcStates = new ArrayList <>();
					}
					integerSrcStates.add(new SrcState(srt.src[i]));
				} else if (colClassNames[i].equals(Long.class.getName())) {
					if (longSrcStates == null) {
						longSrcStates = new ArrayList <>();
					}
					longSrcStates.add(new SrcState(srt.src[i]));
				} else if (colClassNames[i].equals(Float.class.getName())) {
					if (floatSrcStates == null) {
						floatSrcStates = new ArrayList <>();
					}
					floatSrcStates.add(new SrcState(srt.src[i]));
				} else if (colClassNames[i].equals(Double.class.getName())) {
					if (doubleSrcStates == null) {
						doubleSrcStates = new ArrayList <>();
					}
					doubleSrcStates.add(new SrcState(srt.src[i]));
				} else if (colClassNames[i].equals(Timestamp.class.getName())) {
					if (timestampSrcStates == null) {
						timestampSrcStates = new ArrayList <>();
					}
					timestampSrcStates.add(new SrcState(srt.src[i]));
				} else {
					throw new AkUnsupportedOperationException("Not supported column type: " + colClasses[i].getName());
				}
			}
		}

		public SummaryResultTable toSummaryResultTable() throws Exception {
			int numStringCol = 0;
			int numBooleanCol = 0;
			int numIntegerCol = 0;
			int numLongCol = 0;
			int numFloatCol = 0;
			int numDoubleCol = 0;
			int numTimestampCol = 0;
			SummaryResultCol[] srcs = new SummaryResultCol[this.colClassNames.length];
			for (int i = 0; i < colClassNames.length; i++) {
				if (colClassNames[i].equals(String.class.getName())) {
					srcs[i] = this.stringSrcStates.get(numStringCol++).toSummaryResultCol();
				} else if (colClassNames[i].equals(Boolean.class.getName())) {
					srcs[i] = this.booleanSrcStates.get(numBooleanCol++).toSummaryResultCol();
				} else if (colClassNames[i].equals(Integer.class.getName())) {
					srcs[i] = this.integerSrcStates.get(numIntegerCol++).toSummaryResultCol();
				} else if (colClassNames[i].equals(Long.class.getName())) {
					srcs[i] = this.longSrcStates.get(numLongCol++).toSummaryResultCol();
				} else if (colClassNames[i].equals(Float.class.getName())) {
					srcs[i] = this.floatSrcStates.get(numFloatCol++).toSummaryResultCol();
				} else if (colClassNames[i].equals(Double.class.getName())) {
					srcs[i] = this.doubleSrcStates.get(numDoubleCol++).toSummaryResultCol();
				} else if (colClassNames[i].equals(Timestamp.class.getName())) {
					srcs[i] = this.timestampSrcStates.get(numTimestampCol++).toSummaryResultCol();
				} else {
					throw new AkUnsupportedOperationException("Not supported column type: " + colClassNames[i]);
				}
			}

			return new SummaryResultTable(colNames, srcs, dotProduction);
		}

		public static TypeInformation <SrtState> getTypeInfo() throws Exception {
			List <PojoField> pojoFields = new ArrayList <>();
			pojoFields.add(new PojoField(SrtState.class.getField("colNames"), STRING_ARRAY_TYPE_INFO));
			pojoFields.add(new PojoField(SrtState.class.getField("colClassNames"), STRING_ARRAY_TYPE_INFO));
			pojoFields.add(
				new PojoField(SrtState.class.getField("dotProduction"), TypeInformation.of(double[][].class)));

			pojoFields.add(new PojoField(SrtState.class.getField("stringSrcStates"),
				new ListTypeInfo <>(SrcState.getTypeInfo(String.class))));
			pojoFields.add(new PojoField(SrtState.class.getField("booleanSrcStates"),
				new ListTypeInfo <>(SrcState.getTypeInfo(Boolean.class))));
			pojoFields.add(new PojoField(SrtState.class.getField("integerSrcStates"),
				new ListTypeInfo <>(SrcState.getTypeInfo(Integer.class))));
			pojoFields.add(new PojoField(SrtState.class.getField("longSrcStates"),
				new ListTypeInfo <>(SrcState.getTypeInfo(Long.class))));
			pojoFields.add(new PojoField(SrtState.class.getField("floatSrcStates"),
				new ListTypeInfo <>(SrcState.getTypeInfo(Float.class))));
			pojoFields.add(new PojoField(SrtState.class.getField("doubleSrcStates"),
				new ListTypeInfo <>(SrcState.getTypeInfo(Double.class))));
			pojoFields.add(new PojoField(SrtState.class.getField("timestampSrcStates"),
				new ListTypeInfo <>(SrcState.getTypeInfo(Timestamp.class))));

			TypeInformation <SrtState> stateTypeInfo = new PojoTypeInfo <>(SrtState.class, pojoFields);
			return stateTypeInfo;
		}
	}

	/**
	 * Checkpoint state of {@link SummaryResultCol}.
	 */
	public static class SrcState implements Serializable {
		private static final long serialVersionUID = 3754534327522384014L;
		public String dataType;
		public long countTotal;
		public long count;
		public long countMissValue;
		public long countNanValue;
		public long countPositiveInfinity;
		public long countNegativInfinity;
		public double sum;
		public double sum2;
		public double sum3;
		public double sum4;
		public double norm1;
		public Object min;
		public Object max;
		public Object[] topItems;
		public Object[] bottomItems;
		public String colName;

		/* For TreeMap<Object, Long> freq; */
		public Object[] freqKey;
		public Long[] freqValue;

		/* For IntervalCalculator itvcalc; */
		public String itvcalcJson;

		static {
			try {
				String[] fieldsOfSummaryResultCol = new String[] {
					"dataType", "countTotal", "count", "countMissValue", "countNanValue", "countPositiveInfinity",
					"countNegativInfinity", "sum", "sum2", "sum3", "sum4", "norm1",
					"min", "max", "topItems", "bottomItems", "colName", "freq", "itvcalc"
				};
				Set <String> fieldsSet = new HashSet <>(Arrays.asList(fieldsOfSummaryResultCol));
				Field[] fields = SummaryResultCol.class.getDeclaredFields();
				for (Field field : fields) {
					if (Modifier.isFinal(field.getModifiers()) && Modifier.isStatic(field.getModifiers())) {
						continue;
					}
					String name = field.getName();
					if (!fieldsSet.contains(name)) {
						throw new AkIllegalStateException(String.format(
							"Can't find field \"%s\". This exception indicates that fields in SummaryResultCol changed"
								+ " but "
								+
								"SrcState is not updated with SummaryResultCol.", name));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new AkIllegalStateException(e.getMessage());
			}
		}

		public SrcState() { // to make it POJO
		}

		public SrcState(SummaryResultCol src) {
			dataType = src.dataType.getCanonicalName();
			countTotal = src.countTotal;
			count = src.count;
			countMissValue = src.countMissValue;
			countNanValue = src.countNanValue;
			countPositiveInfinity = src.countPositiveInfinity;
			countNegativInfinity = src.countNegativInfinity;
			sum = src.sum;
			sum2 = src.sum2;
			sum3 = src.sum3;
			sum4 = src.sum4;
			norm1 = src.norm1;

			if (src.min != null) {
				min = src.min;
			}
			if (src.max != null) {
				max = src.max;
			}

			topItems = src.topItems;
			bottomItems = src.bottomItems;
			colName = src.colName;

			if (src.freq != null) {
				freqKey = getArray(src.dataType, src.freq.size());
				freqValue = new Long[src.freq.size()];
				int pos = 0;
				for (Map.Entry <Object, Long> entry : src.freq.entrySet()) {
					freqKey[pos] = entry.getKey();
					freqValue[pos] = entry.getValue();
					pos++;
				}
			}

			if (src.itvcalc != null) {
				itvcalcJson = JsonConverter.gson.toJson(src.itvcalc);
			}
		}

		public SummaryResultCol toSummaryResultCol() throws Exception {
			SummaryResultCol src = new SummaryResultCol();
			src.dataType = Class.forName(dataType);
			src.countTotal = countTotal;
			src.count = count;
			src.countMissValue = countMissValue;
			src.countNanValue = countNanValue;
			src.countPositiveInfinity = countPositiveInfinity;
			src.countNegativInfinity = countNegativInfinity;
			src.sum = sum;
			src.sum2 = sum2;
			src.sum3 = sum3;
			src.sum4 = sum4;
			src.norm1 = norm1;
			src.min = min;
			src.max = max;
			src.topItems = topItems;
			src.bottomItems = bottomItems;
			src.colName = colName;
			if (freqKey != null && freqValue != null) {
				src.freq = new TreeMap <>();
				for (int i = 0; i < freqKey.length; i++) {
					src.freq.put(freqKey[i], freqValue[i]);
				}
			}
			if (!StringUtils.isNullOrWhitespaceOnly(itvcalcJson)) {
				src.itvcalc = JsonConverter.gson.fromJson(itvcalcJson, IntervalCalculator.class);
			}
			return src;
		}

		public static TypeInformation getTypeInfo(Class columnDataClass) throws Exception {
			List <PojoField> pojoFields = new ArrayList <>();
			pojoFields.add(new PojoField(SrcState.class.getField("dataType"), AlinkTypes.STRING));
			pojoFields.add(new PojoField(SrcState.class.getField("countTotal"), AlinkTypes.LONG));
			pojoFields.add(new PojoField(SrcState.class.getField("count"), AlinkTypes.LONG));
			pojoFields.add(new PojoField(SrcState.class.getField("countMissValue"), AlinkTypes.LONG));
			pojoFields.add(new PojoField(SrcState.class.getField("countNanValue"), AlinkTypes.LONG));
			pojoFields.add(new PojoField(SrcState.class.getField("countPositiveInfinity"), AlinkTypes.LONG));
			pojoFields.add(new PojoField(SrcState.class.getField("countNegativInfinity"), AlinkTypes.LONG));
			pojoFields.add(new PojoField(SrcState.class.getField("sum"), AlinkTypes.DOUBLE));
			pojoFields.add(new PojoField(SrcState.class.getField("sum2"), AlinkTypes.DOUBLE));
			pojoFields.add(new PojoField(SrcState.class.getField("sum3"), AlinkTypes.DOUBLE));
			pojoFields.add(new PojoField(SrcState.class.getField("sum4"), AlinkTypes.DOUBLE));
			pojoFields.add(new PojoField(SrcState.class.getField("norm1"), AlinkTypes.DOUBLE));
			pojoFields.add(new PojoField(SrcState.class.getField("min"), getDataType(columnDataClass)));
			pojoFields.add(new PojoField(SrcState.class.getField("max"), getDataType(columnDataClass)));
			pojoFields.add(new PojoField(SrcState.class.getField("topItems"), getArrayType(columnDataClass)));
			pojoFields.add(new PojoField(SrcState.class.getField("bottomItems"), getArrayType(columnDataClass)));
			pojoFields.add(new PojoField(SrcState.class.getField("colName"), AlinkTypes.STRING));
			pojoFields.add(new PojoField(SrcState.class.getField("freqKey"), getArrayType(columnDataClass)));
			pojoFields.add(new PojoField(SrcState.class.getField("freqValue"), LONG_ARRAY_TYPE_INFO));
			pojoFields.add(new PojoField(SrcState.class.getField("itvcalcJson"), AlinkTypes.STRING));
			return new PojoTypeInfo <>(SrcState.class, pojoFields);
		}

		public static TypeInformation getArrayType(Class clazz) {
			if (clazz.equals(String.class)) {
				return STRING_ARRAY_TYPE_INFO;
			} else if (clazz.equals(Boolean.class)) {
				return BOOLEAN_ARRAY_TYPE_INFO;
			} else if (clazz.equals(Integer.class)) {
				return INT_ARRAY_TYPE_INFO;
			} else if (clazz.equals(Long.class)) {
				return LONG_ARRAY_TYPE_INFO;
			} else if (clazz.equals(Float.class)) {
				return FLOAT_ARRAY_TYPE_INFO;
			} else if (clazz.equals(Double.class)) {
				return DOUBLE_ARRAY_TYPE_INFO;
			} else if (clazz.equals(Timestamp.class)) {
				return LONG_ARRAY_TYPE_INFO; // we use long array to store the timestamp array
			} else {
				throw new AkUnsupportedOperationException("Not supported data type: " + clazz.getName());
			}
		}

		public static TypeInformation getDataType(Class clazz) {
			if (clazz.equals(Timestamp.class)) {
				return AlinkTypes.LONG;
			} else {
				return TypeInformation.of(clazz);
			}
		}

		public static Object[] getArray(Class clazz, int len) {
			if (clazz.equals(String.class)) {
				return new String[len];
			} else if (clazz.equals(Boolean.class)) {
				return new Boolean[len];
			} else if (clazz.equals(Integer.class)) {
				return new Integer[len];
			} else if (clazz.equals(Long.class)) {
				return new Long[len];
			} else if (clazz.equals(Float.class)) {
				return new Float[len];
			} else if (clazz.equals(Double.class)) {
				return new Double[len];
			} else if (clazz.equals(Timestamp.class)) {
				return new Long[len]; // we use long array to store the timestamp array
			} else {
				throw new AkUnsupportedOperationException("Not supported data type: " + clazz.getName());
			}
		}
	}
}
