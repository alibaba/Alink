package com.alibaba.alink.operator.batch.graph.memory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitionFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * User interface for using memory vertex computation.
 */
public abstract class MemoryComputeFunction implements Serializable {
	private transient MemoryEdgeListGraph graph;
	private transient int numPartitions;
	private transient Collector <GraphCommunicationUnit> out;
	private transient GraphPartitionFunction graphPartitionFunction;
	private transient DistributedGraphContext graphContext;

	private transient final static int MESSAGE_BATCH_SIZE = 32 * 1024 / 128;

	private transient long[][] targetVertexIds;
	private transient double[][] messages;
	private transient int[] curIdx;
	private transient int superStep;

	/**
	 * Gather message for vertexId in to memory store.
	 *
	 * @param vertexId
	 * @param message
	 */
	public abstract void gatherMessage(long vertexId, double message);

	/**
	 * Computes new vertex values based on vertexId and neighbors. Also sends message to its neighbors.
	 *
	 * @param neighborAndValues
	 * @param vertexId
	 */
	public abstract void sendMessage(Iterator <Tuple2 <Long, Double>> neighborAndValues, long vertexId);

	/**
	 * Inits values of all edges. If not overriden, the value of all edges are their weights.
	 * If there is no weights, they are all one.
	 */
	public void initEdgesValues() {

	}

	/**
	 * Inits values of all vertices. If not overriden, the value of all vertices are zero.
	 */
	public void initVerticesValues() {

	}

	public final int getSuperStep() {
		return this.superStep - MemoryVertexCentricIteration.NUM_SETUP_STEPS;
	}

	public final DistributedGraphContext getGraphContext() {
		return graphContext;
	}

	/**
	 * Sends message to target vertex. If target is -1, it means that it needs to be broadcasted to all vertices.
	 *
	 * TODO: Could be optimized to reduce local communication.
	 *
	 * @param target
	 * @param message
	 */
	public final void sendMessageTo(long target, double message) {
		if (target == -1) {
			// broadcast input
			int broadcastId = numPartitions;
			int nextInsertId = curIdx[broadcastId];
			if (nextInsertId == MESSAGE_BATCH_SIZE) {
				for (int pid = 0; pid < numPartitions; pid++) {
					out.collect(new GraphCommunicationUnit(pid, null, messages[broadcastId]));
				}
				curIdx[broadcastId] = 0;
			}
		} else {
			// non-broadcast input
			int pid = graphPartitionFunction.apply(target, numPartitions);
			int nextInsertId = curIdx[pid];
			if (nextInsertId == MESSAGE_BATCH_SIZE) {
				out.collect(new GraphCommunicationUnit(graph.getPhysicalWorkerId(pid), targetVertexIds[pid],
					messages[pid]));
				curIdx[pid] = 0;
				nextInsertId = 0;
			}
			targetVertexIds[pid][nextInsertId] = target;
			messages[pid][nextInsertId] = message;
			curIdx[pid]++;
		}
	}

	/** Below are three utility functions to set/update vertex values and edge values. **/
	public final double getLastStepVertexValue(long vertexId) {
		return graph.getLastStepVertexValue(vertexId);
	}

	public final double getCurVertexValue(long vertexId) {
		return graph.getCurVertexValue(vertexId);
	}

	public final void setCurVertexValue(long vertexId, double newValue) {
		graph.setCurVertexValue(vertexId, newValue);
	}

	public final void incCurVertexValue(long vertexId, double newValue) {
		graph.incCurVertexValue(vertexId, newValue);
	}

	public final void setEdgeValue(long src, long dst, double value) {
		graph.updateEdgeValue(src, dst, value);
	}

	/** Below are utility functions to set/update vertex values and edge values in a batch manner. **/
	public final void setAllVertexValues(double value) {
		graph.setAllVertexValue(value);
	}

	public final void setAllEdgeValues(double value) {
		graph.setAllEdgeValue(value);
	}

	public final void normalizeEdgeValuesByVertex() {
		graph.normalizeEdgeValueByVertex();
	}

	public final void setAllVertexValueByOutDegree() {
		graph.setAllVertexValueByOutDegree();
	}

	/**
	 * Flushes the messages in buffer.
	 */
	final void flushMessages() {
		// non broadcast messages
		for (int pid = 0; pid < numPartitions; pid++) {
			if (curIdx[pid] == 0) {
				continue;
			}
			long[] vertices = Arrays.copyOfRange(targetVertexIds[pid], 0, curIdx[pid]);
			double[] vertexValues = Arrays.copyOfRange(messages[pid], 0, curIdx[pid]);
			out.collect(new GraphCommunicationUnit(graph.getPhysicalWorkerId(pid), vertices, vertexValues));
			curIdx[pid] = 0;
		}
		// broadcast messages
		int broadcastId = numPartitions;
		if (curIdx[broadcastId] != 0) {
			double[] broadcastValues = Arrays.copyOfRange(messages[broadcastId], 0, curIdx[broadcastId]);
			for (int pid = 0; pid < numPartitions; pid++) {
				out.collect(new GraphCommunicationUnit(pid, null, broadcastValues));
			}
		}
	}

	/**
	 * must be called before running.
	 *
	 * @param graph
	 * @param out
	 * @param numPartitions
	 * @param graphPartitionFunction
	 */
	final void setup(MemoryEdgeListGraph graph,
					 Collector <GraphCommunicationUnit> out,
					 int numPartitions,
					 GraphPartitionFunction graphPartitionFunction,
					 DistributedGraphContext graphContext,
					 int superStep) {
		this.graph = graph;
		this.numPartitions = numPartitions;
		this.out = out;
		this.graphPartitionFunction = graphPartitionFunction;
		this.graphContext = graphContext;
		this.superStep = superStep;

		// The last one is used for broadcast.
		targetVertexIds = new long[numPartitions + 1][MESSAGE_BATCH_SIZE];
		messages = new double[numPartitions + 1][MESSAGE_BATCH_SIZE];
		curIdx = new int[numPartitions + 1];
	}
}
