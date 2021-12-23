package com.alibaba.alink.operator.batch.graph.utils;

import java.io.Serializable;
import java.util.List;

/**
 * Graph statistics: (1) for homo- graphs, we include (a) partitionId, (b) number of distinct source vertices (c) number
 * of edges (2) for hete-graphs, we further include (a) number of distinct node types of out vertices.
 */
public class GraphStatistics implements Serializable {
	int partitionId;
	/**
	 * number of distinct source vertices
	 */
	int vertexNum;
	int edgeNum;
	/**
	 * number of distinct node types of out vertices.
	 */
	List <Character> nodeTypes;

	public GraphStatistics(int partitionId, int vertexNum, int edgeNum) {
		this(partitionId, vertexNum, edgeNum, null);
	}

	public GraphStatistics(int partitionId, int vertexNum, int edgeNum, List <Character> nodeTypes) {
		this.partitionId = partitionId;
		this.vertexNum = vertexNum;
		this.edgeNum = edgeNum;
		this.nodeTypes = nodeTypes;
	}

	public int getPartitionId() {
		return partitionId;
	}

	public int getVertexNum() {
		return vertexNum;
	}

	public int getEdgeNum() {
		return edgeNum;
	}

	public List <Character> getNodeTypes() {
		return nodeTypes;
	}
}