package com.alibaba.alink.common.utils.httpsrc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;

public class MovieLensRatings {

	final static String URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/movielens_ratings.csv";
	final static String SCHEMA_STR = "userid bigint, movieid bigint, rating double, timestamp string";

	public static BatchOperator getBatchData() {
		return new CsvSourceBatchOp(URL, SCHEMA_STR);
	}

	public static StreamOperator getStreamData() {
		return new CsvSourceStreamOp(URL, SCHEMA_STR);
	}

	public static String getUserColName() {
		return "userid";
	}

	public static String getItemColName() {
		return "movieid";
	}

	public static String getRateColName() {
		return "rating";
	}
}
