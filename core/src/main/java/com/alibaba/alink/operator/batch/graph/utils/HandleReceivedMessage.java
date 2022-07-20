package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.operator.batch.graph.RandomWalkBatchOp.RandomWalkCommunicationUnit;
import com.alibaba.alink.operator.batch.graph.walkpath.BaseWalkPathEngine;

/**
 * Update the walk path using the received CommunicationUnit from remote.
 */
public class HandleReceivedMessage<T extends RandomWalkCommunicationUnit> extends RichMapPartitionFunction <T, T> {
	long randomWalkStorageHandler;

	public HandleReceivedMessage(long randomWalkStorageHandler) {
		this.randomWalkStorageHandler = randomWalkStorageHandler;
	}

	@Override
	public void mapPartition(Iterable <T> values, Collector <T> out)
		throws Exception {
		if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
			// pass the {hashkey --> partitionId} map to next step.
			for (T val : values) {
				out.collect(val);
			}
		} else {
			int partitionId = getRuntimeContext().getIndexOfThisSubtask();
			BaseWalkPathEngine baseWalkPathEngine = IterTaskObjKeeper.get(randomWalkStorageHandler, partitionId);
			if (baseWalkPathEngine == null) {
				throw new AkIllegalStateException("baseWalkPathEngine is null");
			}

			for (RandomWalkCommunicationUnit randomWalkCommunicationUnit : values) {
				int srcPartitionId = randomWalkCommunicationUnit.getSrcPartitionId();
				assert srcPartitionId == partitionId;
				Long[] verticesSampled = randomWalkCommunicationUnit.getRequestedVertexIds();
				Integer[] walkIds = randomWalkCommunicationUnit.getWalkIds();

				for (int vertexCnt = 0; vertexCnt < verticesSampled.length; vertexCnt++) {
					baseWalkPathEngine.updatePath(walkIds[vertexCnt], verticesSampled[vertexCnt]);
				}
			}
		}
	}
}