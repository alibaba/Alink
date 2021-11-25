package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;

public class EndWritingRandomWalks<IN extends Object> extends RichMapPartitionFunction <IN, long[]> {

	long walkWriteBufferHandler;

	public EndWritingRandomWalks(long walkWriteBufferHandler) {
		this.walkWriteBufferHandler = walkWriteBufferHandler;
	}

	@Override
	public void mapPartition(Iterable <IN> values, Collector <long[]> out)
		throws Exception {
		int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
		for (IN val: values) {
			// block until upstream tasks finish.
		}
		RandomWalkMemoryBuffer memoryBuffer = null;
		for (int pid = 0; pid < numTasks; pid++) {
			memoryBuffer = IterTaskObjKeeper.containsAndRemoves(walkWriteBufferHandler, pid);
			if (memoryBuffer != null) {
				break;
			}
		}
		assert memoryBuffer != null;
		memoryBuffer.writeOneWalk(new long[0]);
	}
}