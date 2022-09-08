package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.LongValue;

import java.util.Arrays;

/**
 * for each edge of undirected graph, return 1. the degree of its source and target
 * 2. the number of triangles based on this edge
 * 3. the quotient of the number of triangles and the min value between the two degrees
 *
 * @author qingzhao
 */

public class EdgeClusterCoefficient {
	public DataSet <Tuple6 <Long, Long, Long, Long, Long, Double>> run(Graph <Long, Double, Double> graph) {
		//calculate the degree of each vertex. Because it is undirected, we only consider inDegree
		DataSet <Tuple2 <Long, Long>> vertexDataSet = graph.inDegrees().map(new Longvalue2Long());
		//construct the output form, and write all the edges in it
		//DataSet<Tuple6<Long, Long, Long, Long, Long, Double>> temp = graph.getEdges().map(new MapEdge());
		//write the degrees of sources and targets in the corresponding position.
		//write degrees of sources (position 0) in position 2. and degrees of targets (position 1) in position 3
		// for convenience of coGroup, put the sources and targets in a Tuple2
		Graph <Long, Double, Long> graphWithDegree = graph
			.joinWithVertices(vertexDataSet, new VerticesJoin())
			.mapEdges(new GraphTempMapEdge());
		//write the neighbors of vertices in their values.
		//We use Set instead of List, because Set is 10%~20% speeder than List
		Graph <Long, Long[], Long> graphTemp = graphWithDegree
			.mapVertices(new GraphTempMapVertex())
			.runScatterGatherIteration(new ScatterGraphTemp(),
				new GatherGraphTemp(),
				1);
		//operate on triplet and write the neighbor number in the values of edges
		return graphTemp
			.getTriplets()
			.map(new MapTriplet());
	}

	public static class Longvalue2Long
		implements MapFunction <Tuple2 <Long, LongValue>, Tuple2 <Long, Long>> {
		private static final long serialVersionUID = -8499849561757464697L;

		@Override
		public Tuple2 <Long, Long> map(Tuple2 <Long, LongValue> value) throws Exception {
			return new Tuple2 <>(value.f0, value.f1.getValue());
		}
	}

	public static class VerticesJoin implements VertexJoinFunction <Double, Long> {
		private static final long serialVersionUID = 3413134536200006612L;

		@Override
		public Double vertexJoin(Double aDouble, Long aLong) {
			return aLong.doubleValue();
		}
	}

	public static class GraphTempMapEdge
		implements MapFunction <Edge <Long, Double>, Long> {
		private static final long serialVersionUID = 5872715320371423887L;

		@Override
		public Long map(Edge <Long, Double> value) throws Exception {
			return 0L;
		}
	}

	public static class GraphTempMapVertex
		implements MapFunction <Vertex <Long, Double>, Long[]> {
		private static final long serialVersionUID = 897641610324504285L;

		@Override
		public Long[] map(Vertex <Long, Double> value) throws Exception {
			return new Long[value.f1.intValue()];
		}
	}

	public static class ScatterGraphTemp
		extends ScatterFunction <Long, Long[], Long, Long> {
		private static final long serialVersionUID = -7538585087831930835L;

		@Override
		public void sendMessages(Vertex <Long, Long[]> vertex) {
			for (Edge <Long, Long> edge : getEdges()) {
				sendMessageTo(edge.getTarget(), vertex.f0);
			}
		}
	}

	public static class GatherGraphTemp
		extends GatherFunction <Long, Long[], Long> {
		private static final long serialVersionUID = 465153561839511086L;

		@Override
		public void updateVertex(Vertex <Long, Long[]> vertex,
								 MessageIterator <Long> inMessages) {
			int count = 0;
			for (Long msg : inMessages) {
				vertex.f1[count] = msg;
				count += 1;
			}
			Arrays.sort(vertex.f1);
			setNewVertexValue(vertex.f1);
		}
	}

	public static class MapTriplet implements MapFunction <
		Triplet <Long, Long[], Long>,
		Tuple6 <Long, Long, Long, Long, Long, Double>> {
		private static final long serialVersionUID = -1837449716800435246L;

		@Override
		public Tuple6 <Long, Long, Long, Long, Long, Double> map(
			Triplet <Long,
				Long[],
				Long> value) throws Exception {
			int l2 = value.f2.length;
			int l3 = value.f3.length;
			int index2 = 0;
			int index3 = 0;
			long count = 0;
			while (index2 < l2 && index3 < l3) {
				if (value.f2[index2].equals(value.f3[index3])) {
					index2 += 1;
					index3 += 1;
					count += 1;
				} else if (value.f2[index2] > value.f3[index3]) {
					index3 += 1;
				} else {
					index2 += 1;
				}
			}
			long f2 = value.f2.length;
			long f3 = value.f3.length;
			return new Tuple6 <>(value.f0, value.f1, f2, f3, count, count * 1. / Math.min(f2, f3));
		}
	}
}