package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Map;

public abstract class FormatWriter implements Serializable {

	private static final long serialVersionUID = 1957286420455596503L;

	public abstract Tuple2 <Boolean, Row> write(Map <String, String> in);

}
