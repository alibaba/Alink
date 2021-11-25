package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;

import java.io.Serializable;

public class ReadFromBufferAndRemoveStaticObject<T extends Object> extends RichMapPartitionFunction <T, long[]>
	implements Serializable {
	long graphStorageHandler;
	long randomWalkStorageHandler;
	long readBufferHandler;
	String delimiter;

	public ReadFromBufferAndRemoveStaticObject(long graphStorageHandler, long randomWalkStorageHandler,
											   long readBufferHandler, String delimiter) {
		this.graphStorageHandler = graphStorageHandler;
		this.randomWalkStorageHandler = randomWalkStorageHandler;
		this.readBufferHandler = readBufferHandler;
		this.delimiter = delimiter;
	}

	@Override
	public void mapPartition(Iterable <T> values, Collector <long[]> out) throws Exception {
		int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();

		for (T val: values) {
			// block until upstream tasks finish
		}
		// try to get the memory buffer.
		RandomWalkMemoryBuffer randomWalkMemoryBuffer = null;
		while (true) {
			for (int pid = 0; pid < numTasks; pid++) {
				randomWalkMemoryBuffer = IterTaskObjKeeper.containsAndRemoves(readBufferHandler, pid);
				if (randomWalkMemoryBuffer != null) {
					break;
				}
			}
			if (randomWalkMemoryBuffer == null) {
				Thread.sleep(1000);
			} else {
				break;
			}
		}
		assert randomWalkMemoryBuffer != null;

		long[] walk = randomWalkMemoryBuffer.readOneWalk();
		while (walk.length != 0) {
			out.collect(walk);
			walk = randomWalkMemoryBuffer.readOneWalk();
		}

		for (int i = 0; i < numTasks; i++) {
			IterTaskObjKeeper.remove(graphStorageHandler, i);
			IterTaskObjKeeper.remove(randomWalkStorageHandler, i);
		}
	}
}