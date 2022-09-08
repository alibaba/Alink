package com.alibaba.alink.operator.batch.graph.memory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

final class MemoryEdgeListGraph {
	/**
	 * Stores global information about the input graph.
	 */
	DistributedGraphContext graphContext;
	/**
	 * Vertex ids on this worker, stored in an ascending order.
	 */
	long[] orderedVertices;
	/**
	 * Used to accelerate getting localIds given a global vertex Id. Could also accessed by ${orderedVertices}.
	 */
	private HashMap <Long, Integer> globalVertexId2LocalId;
	/**
	 * Number of edges on this worker.
	 */
	private final int edgeNum;
	/**
	 * Start index and end index of each vertex in dsts[] and edgeValues.
	 */
	int[] srcEnds;
	/**
	 * Target vertex Ids.
	 */
	long[] dsts;
	/**
	 * Values of each edge.
	 */
	double[] edgeValues;
	/**
	 * Vertex values on this worker at last super step.
	 */
	double[] lastStepVertexValues;
	/**
	 * Current vertex values on this worker.
	 */
	double[] curVertexValues;
	/**
	 * The map from logical workerId to physical worker id.
	 */
	HashMap <Integer, Integer> logicalWorkerId2PhysicalWorkerId;

	/**
	 * Constructs a graph partition in forms of edge list.
	 *
	 * @param orderedVertices The vertices partitioned on this worker in an ascending order. Note that not all vertices
	 *                        in the input edgelist are contained. But the vertices assigned to this worker are
	 *                        contained.
	 * @param edgeNum         Number of edges in the graph partition.
	 */
	MemoryEdgeListGraph(long[] orderedVertices, int edgeNum) {
		this.orderedVertices = orderedVertices;
		this.globalVertexId2LocalId = new HashMap <>(orderedVertices.length);
		for (int i = 0; i < orderedVertices.length; i++) {
			globalVertexId2LocalId.put(orderedVertices[i], i);
		}
		this.edgeNum = edgeNum;
	}

	void setLogicalWorkerId2PhysicalWorkerId(HashMap <Integer, Integer> logicalWorkerId2PhysicalWorkerId) {
		this.logicalWorkerId2PhysicalWorkerId = logicalWorkerId2PhysicalWorkerId;
	}

	int getPhysicalWorkerId(int logicalId) {
		return logicalWorkerId2PhysicalWorkerId.get(logicalId);
	}

	/**
	 * The first one is workerId, which is dummy here.
	 */
	void loadGraph(Iterable <Either <Long, Tuple3 <Long, Long, Double>>> edges) {
		long[] srcs = new long[edgeNum];
		dsts = new long[edgeNum];
		edgeValues = new double[edgeNum];
		int edgeIdx = 0;
		for (Either <Long, Tuple3 <Long, Long, Double>> edge : edges) {
			Tuple3 <Long, Long, Double> e = edge.right();
			srcs[edgeIdx] = e.f0;
			dsts[edgeIdx] = e.f1;
			edgeValues[edgeIdx] = e.f2;
			edgeIdx++;
		}

		// sorts edge list by source
		sortImpl(srcs, dsts, edgeValues, 0, edgeNum - 1);
		srcEnds = new int[orderedVertices.length];
		int srcIdx = 0;
		for (int i = 0; i < srcEnds.length; i++) {
			while (srcIdx < srcs.length && orderedVertices[i] == srcs[srcIdx]) {
				srcIdx++;
			}
			srcEnds[i] = srcIdx;
		}
		lastStepVertexValues = new double[orderedVertices.length];
		curVertexValues = new double[orderedVertices.length];

		// Sorts dst for each vertex
		for (int i = 0; i < srcEnds.length; i++) {
			int localStart = i == 0 ? 0 : srcEnds[i - 1];
			int localEnd = srcEnds[i];
			if (localEnd > localStart) {
				sortImpl(dsts, null, edgeValues, localStart, localEnd - 1);
			}
		}
	}

	void setGraphContext(DistributedGraphContext graphContext) {
		this.graphContext = graphContext;
	}

	int getVertexLocalId(long vertexId) {
		//return Arrays.binarySearch(vertices, vertexId);
		return globalVertexId2LocalId.get(vertexId);
	}

	/**
	 * Updates edge values. If there are duplicate edges, we update all values of the duplicate edges.
	 */
	void updateEdgeValue(long srcId, long dstId, double value) {
		int localId = getVertexLocalId(srcId);
		int start = localId == 0 ? 0 : srcEnds[localId - 1];
		int end = srcEnds[localId];
		int idx = Arrays.binarySearch(dsts, start, end, dstId);
		Preconditions.checkState(idx >= 0);
		if (edgeValues[idx] != value) {
			// not updated yet.
			int duplicateStart = idx;
			while (duplicateStart >= start && dsts[duplicateStart] == dstId) {
				duplicateStart--;
			}
			duplicateStart++;
			int duplicateEnd = idx;
			while (duplicateEnd < end && dsts[duplicateEnd] == dstId) {
				duplicateEnd++;
			}
			Arrays.fill(edgeValues, duplicateStart, duplicateEnd, value);
		}
	}

	double getCurVertexValue(long vertexId) {
		int localId = getVertexLocalId(vertexId);
		return curVertexValues[localId];
	}

	void setCurVertexValue(long vertexId, double value) {
		int localId = getVertexLocalId(vertexId);
		curVertexValues[localId] = value;
	}

	void incCurVertexValue(long vertexId, double value) {
		int localId = getVertexLocalId(vertexId);
		curVertexValues[localId] += value;
	}

	double getLastStepVertexValue(long vertexId) {
		int localId = getVertexLocalId(vertexId);
		return lastStepVertexValues[localId];
	}

	void overrideLastStepVertexValues() {
		System.arraycopy(curVertexValues, 0, lastStepVertexValues, 0, curVertexValues.length);
	}

	void setAllVertexValue(double value) {
		Arrays.fill(curVertexValues, value);
	}

	void setAllEdgeValue(double value) {
		if (edgeValues != null) {
			Arrays.fill(edgeValues, value);
		}
	}

	void normalizeEdgeValueByVertex() {
		for (int localId = 0; localId < srcEnds.length; localId++) {
			int start = localId == 0 ? 0 : srcEnds[localId - 1];
			int end = srcEnds[localId];
			double sum = 0;
			for (int i = start; i < end; i++) {
				sum += edgeValues[i];
			}
			for (int i = start; i < end; i++) {
				edgeValues[i] /= sum;
			}
		}
	}

	void setAllVertexValueByOutDegree() {
		for (int localId = 0; localId < srcEnds.length; localId++) {
			int start = localId == 0 ? 0 : srcEnds[localId - 1];
			int end = srcEnds[localId];
			curVertexValues[localId] = end - start;
		}
	}

	Iterator <Tuple2 <Long, Double>> getNeighborsWithValue(long vertexId) {
		int localId = getVertexLocalId(vertexId);
		int start = localId == 0 ? 0 : srcEnds[localId - 1];
		int end = srcEnds[localId];
		return new NeighborAndValueIterator(start, end);
	}

	private class NeighborAndValueIterator implements Iterator <Tuple2 <Long, Double>> {
		private int start;
		private final int end;

		public NeighborAndValueIterator(int start, int end) {
			this.start = start;
			this.end = end;
		}

		@Override
		public boolean hasNext() {
			return start < end;
		}

		@Override
		public Tuple2 <Long, Double> next() {
			if (start < end) {
				start++;
				return Tuple2.of(dsts[start - 1], edgeValues[start - 1]);
			} else {
				return null;
			}
		}
	}

	/**
	 * Sorts the indices and values using quick sort.
	 */
	private static void sortImpl(long[] index, @Nullable long[] value1, double[] value2, int low, int high) {
		int pivotPos = (low + high) / 2;
		long pivot = index[pivotPos];
		swapIndexAndValue(index, value1, value2, pivotPos, high);

		int pos = low - 1;
		for (int i = low; i <= high; i++) {
			if (index[i] <= pivot) {
				pos++;
				swapIndexAndValue(index, value1, value2, pos, i);
			}
		}
		if (high > pos + 1) {
			sortImpl(index, value1, value2, pos + 1, high);
		}
		if (pos - 1 > low) {
			sortImpl(index, value1, value2, low, pos - 1);
		}
	}

	private static void swapIndexAndValue(long[] index, long[] value1, double[] value2, int index1, int index2) {
		long tempSrc = index[index1];
		index[index1] = index[index2];
		index[index2] = tempSrc;
		if (value1 != null) {
			long tempDst = value1[index1];
			value1[index1] = value1[index2];
			value1[index2] = tempDst;
		}
		double tempValue = value2[index1];
		value2[index1] = value2[index2];
		value2[index2] = tempValue;
	}
}
