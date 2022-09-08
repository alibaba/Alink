package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;

import java.util.HashSet;

/**
 * This class implements a graph algorithm that returns a sub-graph induced by
 * a node list `sources` and their neighbors with at most `depth` hops.
 *
 * @author Fan Hong
 */

public class VertexNeighborSearch implements GraphAlgorithm <String, Long, Long, Graph <String, Long, Long>> {

	private int depth;
	private HashSet <String> sources;

	public VertexNeighborSearch(HashSet <String> sources, int depth) {
		this.sources = sources;
		this.depth = depth;
	}

	@Override
	public Graph <String, Long, Long> run(Graph <String, Long, Long> graph) throws Exception {
		Graph <String, Long, Long> subgraph = graph
			.translateVertexValues(new SetLongMaxValue())
			.runVertexCentricIteration(new VertexNeighborComputeFunction(sources), new MinimumDistanceCombiner(),
				depth + 1)
			.filterOnVertices(new FilterByValue(depth));

		DataSet <Vertex <String, Long>> vertices = subgraph.getVertices();
		return subgraph;
	}

	public static final class SetLongMaxValue implements TranslateFunction <Long, Long> {
		private static final long serialVersionUID = 6439208445273249327L;

		@Override
		public Long translate(Long aLong, Long o) {
			return Long.MAX_VALUE / 2L;
		}
	}

	public static final class FilterByValue implements FilterFunction <Vertex <String, Long>> {
		private static final long serialVersionUID = -3443337881858305297L;
		private int thresh;

		FilterByValue(int thresh) {
			this.thresh = thresh;
		}

		@Override
		public boolean filter(Vertex <String, Long> vertex) {
			return vertex.getValue() <= thresh;
		}
	}

	public static final class VertexNeighborComputeFunction extends ComputeFunction <String, Long, Long, Long> {
		private static final long serialVersionUID = -4927352871373053814L;
		private HashSet <String> sources;

		VertexNeighborComputeFunction(HashSet <String> sources) {
			this.sources = sources;
		}

		@Override
		public void compute(Vertex <String, Long> vertex, MessageIterator <Long> messages) {
			long minDistance = sources.contains(vertex.getId()) ? 0L : Long.MAX_VALUE / 2;

			for (Long msg : messages) {
				minDistance = Math.min(minDistance, msg);
			}

			if (minDistance < vertex.getValue()) {
				setNewVertexValue(minDistance);
				for (Edge <String, Long> e : getEdges()) {
					sendMessageTo(e.getTarget(), minDistance + 1);
				}
			}
		}
	}

	public static final class MinimumDistanceCombiner extends MessageCombiner <String, Long> {

		private static final long serialVersionUID = 1916706983491173310L;

		public void combineMessages(MessageIterator <Long> messages) {

			long minMessage = Long.MAX_VALUE / 2;
			for (Long msg : messages) {
				minMessage = Math.min(minMessage, msg);
			}
			sendCombinedMessage(minMessage);
		}
	}
}

