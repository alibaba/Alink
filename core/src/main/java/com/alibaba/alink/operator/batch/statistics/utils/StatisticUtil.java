package com.alibaba.alink.operator.batch.statistics.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.linalg.DenseVector;

public class StatisticUtil {
	/*
	0: function_name: windowStat, allStat
	1: result_type:srt,ic,...
	2: result_data
	3: timestamp_ms
	4: ext1
	5: ext2
	6: ext3
	 */
	final static public String[] STATISTIC_COL_NAMES = new String[] {"function_name", "result_type", "result_data",
		"timestamp_ms", "ext1", "ext2", "ext3"};
	final static public TypeInformation <?>[] STATISTIC_COL_TYPES = new TypeInformation[] {
		AlinkTypes.STRING, AlinkTypes.STRING,
		AlinkTypes.STRING, AlinkTypes.LONG, AlinkTypes.STRING, AlinkTypes.STRING, AlinkTypes.STRING};
	final static public String COLUMN_NAME_OF_TIMESTAMP = "timestamp";

	/**
	 * determine whether it is a string type.
	 */
	public static boolean isString(String dataType) {
		return "string".equals(dataType.trim().toLowerCase());
	}

	/**
	 * determine whether it is a boolean type.
	 */
	public static boolean isBoolean(String dataType) {
		return "boolean".equals(dataType.trim().toLowerCase());
	}

	/**
	 * determine whether it is a datetime type.
	 */
	public static boolean isDatetime(String dataType) {
		return "datetime".equals(dataType.trim().toLowerCase());
	}

	/**
	 * Transform the vector to {@link DenseVector}, whether the vector is dense or sparse.
	 *
	 * @param obj the input vector
	 * @return the transformed {@link DenseVector}
	 */
	public static double getDoubleValue(Object obj, Class type) {
		if (Number.class.isAssignableFrom(type)) {
			return ((Number) obj).doubleValue();
		} else if (Boolean.class == type) {
			return (Boolean) obj ? 1.0 : 0.0;
		} else if (java.sql.Date.class == type) {
			return (double) ((java.sql.Date) obj).getTime();
		} else if (java.sql.Timestamp.class == type) {
			return (double) ((java.sql.Timestamp) obj).getTime();
		} else if (Byte.class == type) {
			return (double) (((Byte) obj).byteValue());
		} else {
			return 0;
		}
	}

	public static double getDoubleValue(Object obj) {
		return getDoubleValue(obj, obj.getClass());
	}

}
