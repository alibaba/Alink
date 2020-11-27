package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.JsonConverter;

import java.util.Map;

public class JsonWriter extends FormatWriter {

	private static final long serialVersionUID = -1579550176944653654L;

	public JsonWriter() {}

	@Override
	public Tuple2 <Boolean, Row> write(Map <String, String> in) {
		return new Tuple2 <>(true, Row.of(JsonConverter.toJson(in)));
	}
}
