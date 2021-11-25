package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.graph.storage.GraphEdge;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ComputeGraphStatistics extends RichMapPartitionFunction <GraphEdge, GraphStatistics> {

	@Override
	public void mapPartition(Iterable <GraphEdge> values, Collector <GraphStatistics> out)
		throws Exception {
		int edgeNum = 0;
		HashSet <Long> srcVertices = new HashSet <>();
		Set <Character> nodeTypes = new HashSet <>();

		int partitionId = getRuntimeContext().getIndexOfThisSubtask();
		for (GraphEdge e : values) {
			long src = e.getSource();
			srcVertices.add(src);
			edgeNum++;
			if (null != e.getDstType()) {
				nodeTypes.add(e.getDstType());
			}
		}
		out.collect(new GraphStatistics(partitionId, srcVertices.size(), edgeNum, new ArrayList <>(nodeTypes)));
	}
}