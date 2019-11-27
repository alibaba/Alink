package com.alibaba.alink.common.utils.httpsrc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;

/**
 * This is a dataset for association rules mining.
 * see: http://fimi.uantwerpen.be/data/
 */
public class Accidents {

	final static String URL = "http://pai-cj.cn-hangzhou.oss.aliyun-inc.com/csv/accidents.csv";
	final static String SCHEMA_STR = "f string";

	public static BatchOperator getBatchData() {
		return new CsvSourceBatchOp(URL, SCHEMA_STR).setFieldDelimiter("\t");
	}

	public static StreamOperator getStreamData() {
		return new CsvSourceStreamOp(URL, SCHEMA_STR).setFieldDelimiter("\t");
	}

	public static String getColumnName() {
		return "f";
	}
}
