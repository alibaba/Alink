package com.alibaba.alink.operator.batch.graph.walkpath;

import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.operator.batch.graph.Node2VecWalkBatchOp.Node2VecState;

import java.util.Arrays;
import java.util.Iterator;

public class Node2VecWalkPathEngine extends RandomWalkPathEngine {
	/**
	 * state used in rejection sampling.
	 */
	double[] upperBound;
	double[] lowerBound;
	double[] shatter;
	/**
	 * record the prob in one loop
	 */
	double[] prob;
	Node2VecState[] node2VecStates;

	long[] sampledNeighbors;

	public Node2VecWalkPathEngine(int numVertexPerBatch, int numWalksPerVertex, int walkLen,
								  Iterator <Long> vertexPool) {
		super(numVertexPerBatch, numWalksPerVertex, walkLen, vertexPool);
		upperBound = new double[numVertexPerBatch * numWalksPerVertex];
		lowerBound = new double[numVertexPerBatch * numWalksPerVertex];
		shatter = new double[numVertexPerBatch * numWalksPerVertex];
		prob = new double[numVertexPerBatch * numWalksPerVertex];
		// node2vec states are null by default
		node2VecStates = new Node2VecState[numVertexPerBatch * numWalksPerVertex];
		sampledNeighbors = new long[numVertexPerBatch * numWalksPerVertex];

		Arrays.fill(upperBound, Double.MIN_VALUE);
		Arrays.fill(lowerBound, Double.MIN_VALUE);
		Arrays.fill(shatter, Double.MIN_VALUE);
		Arrays.fill(prob, Double.MIN_VALUE);

	}

	public double getUpperBound(int walkId) {
		return upperBound[walkId];
	}

	public void setUpperBound(int walkId, double v) {
		upperBound[walkId] = v;
	}

	public double getLowerBound(int walkId) {
		return lowerBound[walkId];
	}

	public void setLowerBound(int walkId, double v) {
		lowerBound[walkId] = v;
	}

	public double getShatter(int walkId) {
		return shatter[walkId];
	}

	public void setShatter(int walkId, double v) {
		shatter[walkId] = v;
	}

	public double getProb(int walkId) {
		return prob[walkId];
	}

	public void setProb(int walkId, double v) {
		prob[walkId] = v;
	}

	public Node2VecState getNode2VecState(int walkId) {
		return node2VecStates[walkId];
	}

	public void setNode2VecState(int walkId, Node2VecState state) {
		node2VecStates[walkId] = state;
	}

	public void setSampledNeighbor(int walkId, long sampledNeighbor) {
		sampledNeighbors[walkId] = sampledNeighbor;
	}

	public long getSampledNeighbor(int walkId) {
		return sampledNeighbors[walkId];
	}

	/**
	 * utility method
	 *
	 * @param walkId
	 * @param val
	 */
	public void setRejectionState(int walkId, Tuple3 <Double, Double, Double> val) {
		setUpperBound(walkId, val.f0);
		setLowerBound(walkId, val.f1);
		setShatter(walkId, val.f2);
	}

	/**
	 * get previous vertex in the random walk. Used in Node2vec.
	 *
	 * @param walkId
	 */
	public long getPrevVertex(int walkId) {
		if (curPositionIdOfEachWalk[walkId] >= 1) {
			return walks[curPositionIdOfEachWalk[walkId] - 1 + walkId * walkLen];
		} else {
			return -1;
		}
	}
}
