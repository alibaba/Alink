package com.alibaba.alink.common.utils.httpsrc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;

public class MnistDense {

	final static String URL = "http://pai-cj.cn-hangzhou.oss.aliyun-inc.com/csv/mnist_dense.csv";
	final static String SCHEMA_STR = "label bigint, bitmap string";

	public static BatchOperator getBatchData() {
		return new CsvSourceBatchOp(URL, SCHEMA_STR).setFieldDelimiter(";");
	}

	public static StreamOperator getStreamData() {
		return new CsvSourceStreamOp(URL, SCHEMA_STR).setFieldDelimiter(";");
	}

	public static String getLabelColName() {
		return "label";
	}

	public static String getVectorColName() {
		return "bitmap";
	}
}
