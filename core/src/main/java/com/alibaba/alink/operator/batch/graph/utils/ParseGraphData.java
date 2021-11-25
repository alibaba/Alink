package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class ParseGraphData implements FlatMapFunction <Row, Tuple3 <Long, Long, Double>>, Serializable {
	int srcId;
	int dstId;
	/**
	 * -1 means that there is no weight.
	 */
	int weightId;
	boolean isToUndirected;

	public ParseGraphData(int srcId, int dstId, int weightId, boolean isToUndirected) {
		this.srcId = srcId;
		this.dstId = dstId;
		this.weightId = weightId;
		this.isToUndirected = isToUndirected;
	}

	@Override
	public void flatMap(Row value, Collector <Tuple3 <Long, Long, Double>> out) throws Exception {
		long src = ((Number) value.getField(srcId)).longValue();
		long dst = ((Number) value.getField(dstId)).longValue();
		double weight = -1;
		if (weightId != -1) {
			weight = ((Number) value.getField(weightId)).doubleValue();
		}
		out.collect(Tuple3.of(src, dst, weight));
		if (isToUndirected) {
			out.collect(Tuple3.of(dst, src, weight));
		}
	}
}
