package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.graph.EdgeClusterCoefficientParams;

import java.util.Arrays;

@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES))
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "edgeSourceCol", allowedTypeCollections = TypeCollections.INT_LONG_STRING_TYPES)
@ParamSelectColumnSpec(name = "edgeTargetCol", allowedTypeCollections = TypeCollections.INT_LONG_STRING_TYPES)
@NameCn("边聚类系数")
@NameEn("Edge Cluster Coefficient")
public class EdgeClusterCoefficientBatchOp extends BatchOperator <EdgeClusterCoefficientBatchOp>
	implements EdgeClusterCoefficientParams <EdgeClusterCoefficientBatchOp> {

	private static final long serialVersionUID = 7552939759062806711L;

	public EdgeClusterCoefficientBatchOp(Params params) {
		super(params);
	}

	public EdgeClusterCoefficientBatchOp() {
		super(new Params());
	}

	@Override
	public EdgeClusterCoefficientBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();
		String[] outputCols = new String[] {"node1", "node2", "neighbor1", "neighbor2", "commonNeighbor", "edgeClusterCoefficient"};
		Boolean directed = getAsUndirectedGraph();
		String[] inputEdgeCols = new String[] {sourceCol, targetCol};
		TypeInformation <?>[] inputTypes = in.getColTypes();
		int vertexColTypeIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), sourceCol);
		TypeInformation vertexType = inputTypes[vertexColTypeIndex];
		DataSet<Row> inputData = GraphUtilsWithString.input2json(in, inputEdgeCols, 2, false);
		GraphUtilsWithString map = new GraphUtilsWithString(inputData, vertexType);

		DataSet <Edge <Long, Double>> edges = map
			.inputType2longEdge(inputData, false);

		Graph <Long, Double, Double> graph;
		if (directed) {
			graph = Graph.fromDataSet(edges, MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment())
				.mapVertices(new MapVertices()).getUndirected();
		} else {
			graph = Graph.fromDataSet(edges, MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment())
				.mapVertices(new MapVertices());
		}
		//distinct the output undirected edges.
		DataSet <Tuple6 <Long, Long, Long, Long, Long, Double>> resTuple = new EdgeClusterCoefficient().run(graph)
			.groupBy(0)
			.reduceGroup(
				new GroupReduceFunction <Tuple6 <Long, Long, Long, Long, Long, Double>, Tuple6 <Long, Long, Long,
					Long, Long, Double>>() {
					private static final long serialVersionUID = 2757693152062829979L;

					@Override
					public void reduce(Iterable <Tuple6 <Long, Long, Long, Long, Long, Double>> values,
									   Collector <Tuple6 <Long, Long, Long, Long, Long, Double>> out) throws
						Exception {
						for (Tuple6 <Long, Long, Long, Long, Long, Double> i : values) {
							if (i.f0 > i.f1) {
								out.collect(i);
							}
						}
					}
				});
		DataSet <Row> res = map.long2outputTypeECC(resTuple);
		this.setOutput(res, outputCols,
			new TypeInformation <?>[] {vertexType, vertexType, Types.LONG,
				Types.LONG, Types.LONG, Types.DOUBLE});
		return this;
	}

	public static class MapVertices implements MapFunction <Vertex <Long, NullValue>, Double> {
		private static final long serialVersionUID = -2462169842349628527L;

		@Override
		public Double map(Vertex <Long, NullValue> value) throws Exception {
			return 1.;
		}
	}

	/**
	 * for each edge of undirected graph, return 1. the degree of its source and target
	 * 2. the number of triangles based on this edge
	 * 3. the quotient of the number of triangles and the min value between the two degrees
	 */

	public static class EdgeClusterCoefficient {
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
}
