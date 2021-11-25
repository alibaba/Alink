package com.alibaba.alink.operator.batch.graph.walkpath;

import java.util.Arrays;

public abstract class BaseWalkPathEngine {
	final int numVertexPerBatch, numWalksPerVertex, walkLen;
	/**
	 * used to store all of the random walks
	 */
	final long[] walks;

	/**
	 * record the position of each random walk.
	 * the vertex of this position is already sampled.
	 * it is -1 if there is no random walk here.
	 */
	final int[] curPositionIdOfEachWalk;
	/**
	 * temp variable frequently used, persist it here to avoid frequent GC.
	 */
	final long[] idsOfNextBatchOfVerticesToSampleFrom;

	public BaseWalkPathEngine(int numVertexPerBatch, int numWalksPerVertex, int walkLen) {
		this.numVertexPerBatch = numVertexPerBatch;
		this.numWalksPerVertex = numWalksPerVertex;
		this.walkLen = walkLen;
		walks = new long[numVertexPerBatch * numWalksPerVertex * walkLen];
		Arrays.fill(walks, -1);

		curPositionIdOfEachWalk = new int[numVertexPerBatch * numWalksPerVertex];
		Arrays.fill(curPositionIdOfEachWalk, -1);
		idsOfNextBatchOfVerticesToSampleFrom = new long[numVertexPerBatch * numWalksPerVertex];
		Arrays.fill(idsOfNextBatchOfVerticesToSampleFrom, -1);
	}

	/**
	 * update one random walk with a new value.
	 *
	 * @param walkId
	 * @param newValue
	 */
	public void updatePath(int walkId, long newValue) {
		curPositionIdOfEachWalk[walkId]++;
		walks[walkId * walkLen + curPositionIdOfEachWalk[walkId]] = newValue;
	}

	/**
	 * whether this walk can finished: (1) if there is a random walk here (2) this random walk has been finished
	 *
	 * @param walkId
	 * @return
	 */
	public boolean canOutput(int walkId) {
		if (curPositionIdOfEachWalk[walkId] == -1) {
			// there is no random walk here, i.e., the buffer is not fully utilized and the sampling process is
			// supposed to finish fast
			return false;
		}
		if (curPositionIdOfEachWalk[walkId] == walkLen - 1) {
			// achieved path end
			return true;
		}
		if (walks[walkId * walkLen + curPositionIdOfEachWalk[walkId]] == -1 && curPositionIdOfEachWalk[walkId] != 0) {
			// early stop and run into a vertex with no outgoing neighbors
			return true;
		}
		return false;
	}

	/**
	 * If the users find that ${walkId}-th random walk has been finished, it can call this method to get it and add a
	 * new random walk in.
	 *
	 * @param walkId
	 * @return
	 */
	public abstract long[] getOneWalkAndAddNewWalk(int walkId);
}
