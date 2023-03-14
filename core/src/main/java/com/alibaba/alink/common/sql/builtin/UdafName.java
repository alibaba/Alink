package com.alibaba.alink.common.sql.builtin;

public enum UdafName {
	COUNT("count"),
	SUM("sum"),
	AVG("avg"),
	MIN("min"),
	MAX("max"),
	STD_SAMP("stddev_samp"),
	STD_POP("stddev_pop"),
	VAR_SAMP("var_samp"),
	VAR_POP("var_pop"),
	SKEWNESS("skewness"),
	RANK("rank"),
	DENSE_RANK("dense_rank"),
	ROW_NUMBER("row_number"),
	LAG("lag"),
	LAST_DISTINCT("last_distinct"),
	LAST_TIME("last_time"),

	//last_value is conflict in blink, so last_value_impl. and transform.
	LAST_VALUE("last_value_impl"),

	LAST_VALUE_CONSIDER_NULL("last_value_including_null"),
	LISTAGG("listagg"),
	MODE("mode"),
	SUM_LAST("sum_last"),
	SQUARE_SUM("square_sum"),
	MEDIAN("median"),
	FREQ("freq"),
	IS_EXIST("is_exist"),
	TIMESERIES_AGG("timeseries_agg"),
	MTABLE_AGG("mtable_agg"),
	CONCAT_AGG("concat_agg");

	public String name;

	UdafName(String name) {
		this.name = name;
	}
}
