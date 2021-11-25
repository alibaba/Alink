package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.java.functions.KeySelector;

import com.alibaba.alink.operator.batch.graph.RandomWalkBatchOp.RandomWalkCommunicationUnit;

public class RecvRequestKeySelector<T extends RandomWalkCommunicationUnit> implements KeySelector <T, Long> {
	@Override
	public Long getKey(RandomWalkCommunicationUnit value) throws Exception {
		return (long) value.getSrcPartitionId();
	}
}