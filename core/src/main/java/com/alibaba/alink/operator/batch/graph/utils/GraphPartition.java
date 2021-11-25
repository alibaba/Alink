package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.Partitioner;

public class GraphPartition {
	public interface GraphPartitionFunction extends Function {
		/**
		 * compute which partition should this key be partitioned to
		 * @param key
		 * @param numPartitions
		 * @return
		 */
		int apply(long key, int numPartitions);
	}

	/**
	 * Hash function used to partition the vertices.
	 */
	public static class GraphPartitionHashFunction implements GraphPartitionFunction {

		@Override
		public int apply(long key, int numPartitions) {
			return (int) (key % numPartitions);
		}
	}

	public static class GraphPartitioner implements Partitioner <Long> {
		GraphPartitionFunction graphPartitionFunction;

		public GraphPartitioner(GraphPartitionFunction graphPartitionFunction) {
			this.graphPartitionFunction = graphPartitionFunction;
		}

		@Override
		public int partition(Long key, int numPartitions) {
			return graphPartitionFunction.apply(key, numPartitions);
		}
	}
}
