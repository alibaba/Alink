package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

import com.alibaba.alink.operator.batch.graph.storage.GraphEdge;

public class ConstructHeteEdge implements MapFunction <Tuple5 <Long, Long, Double, Character, Character>, GraphEdge> {

	@Override
	public GraphEdge map(Tuple5 <Long, Long, Double, Character, Character> value) throws Exception {
		return new GraphEdge(value.f0, value.f1, value.f2, value.f3, value.f4);
	}
}