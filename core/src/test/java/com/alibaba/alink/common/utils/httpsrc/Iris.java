package com.alibaba.alink.common.utils.httpsrc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;

public class Iris {

	final static String URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv";
	final static String SCHEMA_STR
		= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

	public static BatchOperator getBatchData() {
		return new CsvSourceBatchOp(URL, SCHEMA_STR);
	}

	public static StreamOperator getStreamData() {
		return new CsvSourceStreamOp(URL, SCHEMA_STR);
	}

	public static String getLabelColName() {
		return "category";
	}

	public static String[] getFeatureColNames() {
		return new String[] {"sepal_length", "sepal_width", "petal_length", "petal_width"};
	}
}
