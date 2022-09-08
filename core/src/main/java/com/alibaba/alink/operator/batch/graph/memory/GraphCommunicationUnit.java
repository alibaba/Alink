package com.alibaba.alink.operator.batch.graph.memory;

import java.io.Serializable;

/**
 * The communication unit for memory vertex-centric iteration implementation.
 */
class GraphCommunicationUnit implements Serializable {
	/**
	 * The worker id this message is sent to.
	 */
	public final int targetWorkerId;
	/**
	 * The vertex ids.
	 */
	public final long[] vertexIds;
	/**
	 * The vertex values.
	 */
	public final double[] vertexValues;

	public GraphCommunicationUnit(int targetWorkerId, long[] vertexIds,
								  double[] vertexValues) {
		this.targetWorkerId = targetWorkerId;
		this.vertexIds = vertexIds;
		this.vertexValues = vertexValues;
	}
}