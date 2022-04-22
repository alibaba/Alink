package com.alibaba.alink.operator.batch.graph.walkpath;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * RandomWalkStorage is used to store ${numWalksPerVertex} * ${numVertex} random walks in a compact format.
 */
public class MetaPathWalkPathEngine extends BaseWalkPathEngine {
	/**
	 * all metapaths
	 */
	final List <char[]> metaPaths;
	/**
	 * record the metapath of each random walk that it is following
	 */
	int[] metaPathIdOfEachWalk;

	/**
	 * used to extract vertex from. <vertexId, nodeType>
	 */
	final Iterator <Tuple2 <Long, Character>> vertexPool;
	/**
	 * the vertex a new random walk to start from
	 */
	Tuple2 <Long, Character> curSamplingVertexAndItsType;
	/**
	 * number of random walks starting ${curSamplingVertex} that have not been sampled
	 */
	int curSamplingVertexLeft;
	/**
	 * the metaPathId next to sample from
	 */
	int nextMetaPathId;

	/**
	 * next batch of vertices and its corresponding sampling types.
	 * temp variable frequently used, persist it here to avoid frequent GC.
	 */
	final Character[] typesOfNextBatchOfVerticesToSampleFrom;

	/**
	 * used to faster get valid vertices.
	 */
	Set <Character> validStartVertices;

	public MetaPathWalkPathEngine(int numVertexPerBatch, int numWalksPerVertex, int walkLen,
								  Iterator <Tuple2 <Long, Character>> vertexPool, List <char[]> metaPaths) {
		super(numVertexPerBatch, numWalksPerVertex, walkLen);

		this.vertexPool = vertexPool;
		this.metaPaths = metaPaths;
		metaPathIdOfEachWalk = new int[numVertexPerBatch * numWalksPerVertex];
		Arrays.fill(metaPathIdOfEachWalk, -1);
		validStartVertices = new HashSet <>();
		for (int i = 0; i < metaPaths.size(); i++) {
			validStartVertices.add(metaPaths.get(i)[0]);
		}

		curSamplingVertexLeft = 0;
		curSamplingVertexAndItsType = null;
		nextMetaPathId = metaPaths.size();
		typesOfNextBatchOfVerticesToSampleFrom = new Character[numVertexPerBatch * numWalksPerVertex];
		initWalks();
	}

	/**
	 * initialize all the random walks in this batch. Called only in the construction method.
	 */
	private void initWalks() {
		for (int walkId = 0; walkId < numVertexPerBatch * numWalksPerVertex; walkId++) {
			Tuple2 <Integer, Long> metaPathIdAndVertexId = getNextWalkStartVertexFromPool();
			// in metapath, #randomWalk may be less than ${numVertexPerBatch * numWalksPerVertex}
			if (null != metaPathIdAndVertexId) {
				walks[walkId * walkLen] = metaPathIdAndVertexId.f1;
				metaPathIdOfEachWalk[walkId] = metaPathIdAndVertexId.f0;
				curPositionIdOfEachWalk[walkId] = 0;
			} else {
				return;
			}
		}
	}

	/**
	 * get the next start vertex and its metaPathId from the vertexPool for sampling Will filter the verteices that are
	 * not compatible with provided methpaths
	 *
	 * @return null if all vertices has been sampled, the vertexId to start from otherwise.
	 */
	private Tuple2 <Integer, Long> getNextWalkStartVertexFromPool() {
		if (curSamplingVertexLeft == 0 && nextMetaPathId == metaPaths.size()) {
			if (vertexPool.hasNext()) {
				curSamplingVertexAndItsType = vertexPool.next();
				// filter invalid vertices.
				while (!validStartVertices.contains(curSamplingVertexAndItsType.f1) && vertexPool.hasNext()) {
					curSamplingVertexAndItsType = vertexPool.next();
				}
				if (!validStartVertices.contains(curSamplingVertexAndItsType.f1)) {
					return null;
				} else {
					curSamplingVertexLeft = numWalksPerVertex;
					nextMetaPathId = 0;
				}
			} else {
				return null;
			}
		}
		while (curSamplingVertexLeft > 0) {
			while (nextMetaPathId < metaPaths.size()) {
				nextMetaPathId++;
				if (curSamplingVertexAndItsType.f1.equals(metaPaths.get(nextMetaPathId - 1)[0])) {
					return Tuple2.of(nextMetaPathId - 1, curSamplingVertexAndItsType.f0);
				}
			}
			// finished one walk of one vertex
			nextMetaPathId = 0;
			curSamplingVertexLeft--;
		}
		// finish one vertex
		curSamplingVertexLeft = 0;
		nextMetaPathId = metaPaths.size();
		return getNextWalkStartVertexFromPool();
	}

	@Override
	public long[] getOneWalkAndAddNewWalk(int walkId) {

		int localStart = walkId * walkLen;
		int localEnd = localStart + walkLen;
		while (localEnd > localStart && walks[localEnd - 1] == -1) {
			localEnd --;
		}
		int validLen = localEnd - localStart;
		Preconditions.checkState(validLen > 0, "walkLen should be greater than zero.");
		long[] finishedRandomWalk = new long[validLen];
		System.arraycopy(walks, localStart, finishedRandomWalk, 0, validLen);
		Arrays.fill(walks, localStart, localStart + walkLen, -1L);

		Tuple2 <Integer, Long> metaPathIdAndVertexId = getNextWalkStartVertexFromPool();
		if (null != metaPathIdAndVertexId) {
			walks[localStart] = metaPathIdAndVertexId.f1;
			metaPathIdOfEachWalk[walkId] = metaPathIdAndVertexId.f0;
			curPositionIdOfEachWalk[walkId] = 0;
		} else {
			walks[localStart] = -1;
			metaPathIdOfEachWalk[walkId] = -1;
			curPositionIdOfEachWalk[walkId] = -1;
		}

		return finishedRandomWalk;
	}

	/**
	 * for each random walk, Tuple<long, char> means that we need to sample ${vertexId}'s neighbor with ${char} type.
	 *
	 * @return
	 */
	public Tuple2 <long[], Character[]> getNextBatchOfVerticesToSampleFrom() {
		for (int walkId = 0; walkId < numVertexPerBatch * numWalksPerVertex; walkId++) {
			if (metaPathIdOfEachWalk[walkId] != -1) {
				idsOfNextBatchOfVerticesToSampleFrom[walkId] = walks[curPositionIdOfEachWalk[walkId]
					+ walkId * walkLen];
				int curPositionOfThisWalk = curPositionIdOfEachWalk[walkId];

				typesOfNextBatchOfVerticesToSampleFrom[walkId] = getNextTypeGivenCurPositionAndMetaPath(
					curPositionOfThisWalk + 1, metaPaths.get(metaPathIdOfEachWalk[walkId]));

			} else {
				idsOfNextBatchOfVerticesToSampleFrom[walkId] = -1L;
				typesOfNextBatchOfVerticesToSampleFrom[walkId] = null;
			}
		}
		return Tuple2.of(idsOfNextBatchOfVerticesToSampleFrom, typesOfNextBatchOfVerticesToSampleFrom);
	}

	/**
	 * return next vertex to sample from and its out neighbors
	 *
	 * @param walkId
	 * @return
	 */
	public Tuple2 <Long, Character> getNextVertexToSampleFrom(int walkId) {
		// this walkId-corresponded random walk is not used to store the random walk.
		if (metaPathIdOfEachWalk[walkId] == -1 || curPositionIdOfEachWalk[walkId] == -1) {
			return null;
		}
		int curPosition = curPositionIdOfEachWalk[walkId];
		long nextVertex = walks[curPosition + walkId * walkLen];
		if (nextVertex == -1) {
			// already gets to the end
			return null;
		}
		char nextType = getNextTypeGivenCurPositionAndMetaPath(curPosition + 1,
			metaPaths.get(metaPathIdOfEachWalk[walkId]));
		return Tuple2.of(nextVertex, nextType);
	}

	private char getNextTypeGivenCurPositionAndMetaPath(int nextPostionOfOneWalk, char[] metaPath) {
		int nextTypeId = nextPostionOfOneWalk % (metaPath.length - 1);
		return metaPath[nextTypeId];
	}

}