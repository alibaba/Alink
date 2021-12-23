package com.alibaba.alink.operator.batch.graph.walkpath;

import java.util.Arrays;
import java.util.Iterator;

/**
 * RandomWalkStorage is used to store ${numWalksPerVertex} * ${numVertex} random walks in a compact format.
 */
public class RandomWalkPathEngine extends BaseWalkPathEngine {
	/**
	 * used to extract start vertices
	 */
	final Iterator <Long> vertexPool;
	/**
	 * the vertex a new random walk to start from
	 */
	long curSamplingVertex;
	/**
	 *  number of random walks starting ${curSamplingVertex} that have not been sampled
	 */
	int curSamplingVertexLeft;

	public RandomWalkPathEngine(int numVertexPerBatch, int numWalksPerVertex, int walkLen, Iterator <Long> vertexPool) {
		super(numVertexPerBatch, numWalksPerVertex, walkLen);

		this.vertexPool = vertexPool;
		curSamplingVertexLeft = 0;
		curSamplingVertex = -1;

		initWalks();
	}

	/**
	 * initialize all the random walks in this batch. Called only in the construction method.
	 */
	private void initWalks() {
		for (int walkId = 0; walkId < numVertexPerBatch * numWalksPerVertex; walkId++) {
			walks[walkId * walkLen] = getNextWalkStartVertexFromPool();
			curPositionIdOfEachWalk[walkId] = 0;
		}
	}

	/**
	 * get the next start vertex from the vertexPool for sampling
	 *
	 * @return -1 if all vertices has been sampled, the vertexId to start from otherwise.
	 */
	private long getNextWalkStartVertexFromPool() {
		if (curSamplingVertexLeft == 0) {
			if (vertexPool.hasNext()) {
				curSamplingVertex = vertexPool.next();
				curSamplingVertexLeft = numWalksPerVertex;
			} else {
				return -1;
			}
		}
		curSamplingVertexLeft--;
		return curSamplingVertex;
	}

	@Override
	public long[] getOneWalkAndAddNewWalk(int walkId) {
		int localStart = walkId * walkLen;
		int localEnd = localStart + walkLen;
		while (localEnd > localStart && walks[localEnd - 1] == -1) {
			localEnd --;
		}
		int validLen = localEnd - localStart;
		assert validLen > 0;
		long[] finishedRandomWalk = new long[validLen];
		System.arraycopy(walks, localStart, finishedRandomWalk, 0, validLen);
		Arrays.fill(walks, localStart, localStart + walkLen, -1L);
		long nextStartVertex = getNextWalkStartVertexFromPool();
		walks[localStart] = nextStartVertex;
		if (nextStartVertex != -1) {
			curPositionIdOfEachWalk[walkId] = 0;
		} else {
			curPositionIdOfEachWalk[walkId] = -1;
		}
		return finishedRandomWalk;
	}

	public long[] getNextBatchOfVerticesToSampleFrom() {
		for (int walkId = 0; walkId < numVertexPerBatch * numWalksPerVertex; walkId++) {
			idsOfNextBatchOfVerticesToSampleFrom[walkId] = getNextVertexToSampleFrom(walkId);
		}
		return idsOfNextBatchOfVerticesToSampleFrom;
	}

	public long getNextVertexToSampleFrom(int walkId) {
		if (curPositionIdOfEachWalk[walkId] == -1) {
			return -1L;
		}
		return walks[curPositionIdOfEachWalk[walkId] + walkId * walkLen];
	}
}