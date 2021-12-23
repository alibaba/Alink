package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.operator.batch.graph.storage.GraphEdge;

public class ConstructHomoEdge implements MapFunction <Tuple3 <Long, Long, Double>, GraphEdge> {

	@Override
	public GraphEdge map(Tuple3 <Long, Long, Double> value) throws Exception {
		return new GraphEdge(value.f0, value.f1, value.f2);
	}
}