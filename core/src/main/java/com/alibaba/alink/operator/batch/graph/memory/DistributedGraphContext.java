package com.alibaba.alink.operator.batch.graph.memory;

public class DistributedGraphContext {
	/**
	 * Number of vertices in the whole graph.
	 */
	public final long numVertex;
	/**
	 * Number of edges in the whole graph.
	 */
	public final long numEdge;

	public DistributedGraphContext(long numVertex, long numEdge) {
		this.numVertex = numVertex;
		this.numEdge = numEdge;
	}

}
