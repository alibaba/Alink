package com.alibaba.alink.operator.batch.graph.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * abstract class for storing homo & hete graphs.
 */
public abstract class BaseCSRGraph {
	/**
	 * only vertex with outgoing edges will be put in this map.
	 */
	protected HashMap <Long, Integer> srcVertexId2LocalId;
	protected int[] srcEnds;
	protected long[] dst;

	protected boolean isWeighted;
	protected double[] weights;

	/**
	 * used by partial sampling
	 */
	double[] partialSum;

	/**
	 * used by alias table
	 */
	double[] prob;
	int[] alias;
	boolean useAliasTable;

	Random random;

	Map <Integer, Integer> logicalWorkerIdToPhysicalWorkerId;

	/**
	 * Requirement: the input edges are sorted by srcId, then dstType if dstType is not null.
	 *
	 * @param srcVertexNum  number of source vertices
	 * @param edgeNum       number of edges
	 * @param isWeighted    whether it is a weighted graph or not
	 * @param useAliasTable whether use alias table
	 * @Param numNodeTypes number of node types for each node, default is one.
	 */
	public BaseCSRGraph(int srcVertexNum, int edgeNum, boolean isWeighted, boolean useAliasTable,
						int numNodeTypes) {
		int initialCap = srcVertexNum * 3 / 2;
		this.srcVertexId2LocalId = new HashMap <>(initialCap);
		this.srcEnds = new int[srcVertexNum * numNodeTypes];
		this.dst = new long[edgeNum];
		this.isWeighted = isWeighted;

		this.useAliasTable = useAliasTable;
		this.random = new Random(2021);
		if (isWeighted) {
			weights = new double[edgeNum];
			if (useAliasTable) {
				alias = new int[edgeNum];
				//prob = new double[edgeNum];
				// reuse memory in this.weights
				prob = weights;
			} else {
				//partialSum = new double[edgeNum];
				// reuse memory in this.weights
				partialSum = weights;
			}
		}
	}

	/**
	 * build alias table or partial sum for one vertex's neighbors or its neighbors with a specific node type.
	 *
	 * @param localId
	 */
	protected void buildOneVertexPartialSumOrAliasTable(int localId) {
		int localStart = localId == 0 ? 0 : srcEnds[localId - 1];
		int localEnd = srcEnds[localId];
		// [localStart, end) is the edge for node ${localId}
		if (useAliasTable) {
			GraphUtils.buildAliasTable(localStart, localEnd, alias, prob);
		} else {
			GraphUtils.buildPartialSum(localStart, localEnd, partialSum);
		}

	}

	public BaseCSRGraph(int srcVertexNum, int edgeNum, boolean isWeighted, boolean useAliasTable) {
		this(srcVertexNum, edgeNum, isWeighted, useAliasTable, 1);
	}

	public Iterator <Long> getAllSrcVertices() {
		return srcVertexId2LocalId.keySet().iterator();
	}

	public boolean containsVertex(long vertexId) {
		return srcVertexId2LocalId.containsKey(vertexId);
	}

	public void setLogicalWorkerIdToPhysicalWorkerId(Map <Integer, Integer> logicalWorkerIdToPhysicalWorkerId) {
		this.logicalWorkerIdToPhysicalWorkerId = logicalWorkerIdToPhysicalWorkerId;
	}

	/**
	 * return the physical worker id given the logical worker id.
	 * If a logical worker id does not exist in the map, there is no partitioned graph on that logical worker.
	 * We return 0 as the default worker.
	 *
	 * @param logicalWorkerId
	 * @return
	 */
	public int getPhysicalWorkerIdByLogicalWorkerId(int logicalWorkerId) {
		assert logicalWorkerId >= 0;
		if (!logicalWorkerIdToPhysicalWorkerId.containsKey(logicalWorkerId)) {
			return 0;
		}
		else {
			return logicalWorkerIdToPhysicalWorkerId.get(logicalWorkerId);
		}
	}
}