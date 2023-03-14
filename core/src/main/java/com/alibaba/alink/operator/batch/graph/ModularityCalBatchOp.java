package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

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
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.graph.ModularityCalParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

@InputPorts(values = {
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES),
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRAPH_VERTICES),
})
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "vertexCol", portIndices = 1)
@ParamSelectColumnSpec(name = "vertexCommunityCol", portIndices = 1, allowedTypeCollections = TypeCollections.INT_LONG_TYPES)
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeWeightCol", portIndices = 0)
@NameCn("模块度计算")
@NameEn("Calculate Modularity")
public class ModularityCalBatchOp extends BatchOperator <ModularityCalBatchOp>
	implements ModularityCalParams <ModularityCalBatchOp> {
	private static final long serialVersionUID = -7765756516724178687L;

	public ModularityCalBatchOp(Params params) {
		super(params);
	}

	public ModularityCalBatchOp() {
		super(new Params());
	}

	public ModularityCalBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();
		String edgeWeightCol = getEdgeWeightCol();
		boolean hasEdgeWeightCol = !(edgeWeightCol == null);
		String outCol = "modularity";
		String vertexCol = getVertexCol();
		String vertexCommunityCol = getVertexCommunityCol();
		Boolean directed = getAsUndirectedGraph();
		String[] inputEdgeCols = hasEdgeWeightCol ? new String[] {sourceCol, targetCol, edgeWeightCol}
			: new String[] {sourceCol, targetCol};
		String[] inputVertexCols = new String[] {vertexCol, vertexCommunityCol};
		DataSet <Row> inputEdgeData = inputs[0].select(inputEdgeCols).getDataSet();
		DataSet <Row> inputVertexData = inputs[1].select(inputVertexCols).getDataSet();

		DataSet<Tuple2 <String, Long>> labelMapping = GraphUtils.graphNodeIdMapping(inputVertexData, new int[]{1}, null, 0);

		DataSet <Edge <String, Double>> edges = GraphUtils.rowToEdges(inputEdgeData, hasEdgeWeightCol, directed);
		inputVertexData = GraphUtils.mapOriginalToId(inputVertexData, labelMapping, new int[]{1});
		DataSet <Vertex <String, Long>> vertices = inputVertexData.map(new MapFunction <Row, Vertex <String, Long>>() {
			@Override
			public Vertex <String, Long> map(Row value) throws Exception {
				return new Vertex <>(String.valueOf(value.getField(0)), (Long) value.getField(1));
			}
		});

		DataSet<Tuple3 <Long, Double, Double>> vertexLabelDataSet = edges.join(vertices).where(0).equalTo(0).with(
			new JoinFunction <Edge <String, Double>, Vertex <String, Long>, Tuple3<Long, String, Double>>() {
				@Override
				public Tuple3<Long, String, Double> join(Edge <String, Double> first, Vertex <String, Long> second)
					throws Exception {
					return Tuple3.of(second.f1, first.f1, first.f2);
				}
			}).join(vertices).where(1).equalTo(0).with(
			new JoinFunction <Tuple3<Long, String, Double>, Vertex <String, Long>, Tuple3<Long, Double, Double>>() {
				@Override
				public Tuple3<Long, Double, Double> join(Tuple3<Long, String, Double> first, Vertex <String, Long> second)
					throws Exception {
					if (first.f0.equals(second.f1)) {
						return Tuple3.of(first.f0, first.f2, first.f2);
					} else {
						return Tuple3.of(first.f0, 0.0D, first.f2);
					}
				}
			});

		DataSet <Row> res = ModularityCal.modularity(vertexLabelDataSet)
			.map(new MapFunction <Tuple1 <Double>, Row>() {
				private static final long serialVersionUID = -6930741669256207575L;

				@Override
				public Row map(Tuple1 <Double> value) throws Exception {
					Row r = new Row(1);
					r.setField(0, value.f0);
					return r;
				}
			});
		this.setOutput(res, new String[] {outCol}, new TypeInformation <?>[] {Types.DOUBLE});
		return this;
	}

	/**
	 * This algorithm calculates the modularity of the input graph.
	 * The values of vertices of input graph represent the community label.
	 * The edges represent connect relationship between vertices.
	 */
	public static class ModularityCal {

		public static DataSet <Tuple1 <Double>> modularity(DataSet <Tuple3<Long, Double, Double>> groupWeights) {
			DataSet <Tuple1 <Double>> m = groupWeights.aggregate(Aggregations.SUM, 2).project(2);
			DataSet <Tuple3<Long, Double, Double>> weights = groupWeights.groupBy(0)
				.aggregate(Aggregations.SUM, 1)
				.and(Aggregations.SUM, 2);
			DataSet <Tuple1 <Double>> modularity = weights.reduceGroup(new RichGroupReduceFunction <Tuple3 <Long, Double, Double>, Tuple1<Double>>() {
				@Override
				public void reduce(Iterable <Tuple3 <Long, Double, Double>> values, Collector <Tuple1 <Double>> out)
					throws Exception {
					double m = ((Tuple1 <Double>) getRuntimeContext().getBroadcastVariable("m").get(0)).f0;
					double inGroup = 0;
					double outGroup = 0;
					for (Tuple3 <Long, Double, Double> value : values) {
						inGroup += value.f1;
						outGroup += Math.pow(value.f2, 2);
					}
					out.collect(Tuple1.of(inGroup / m - outGroup / Math.pow(m, 2)));
				}
			}).withBroadcastSet(m, "m");
			return modularity;
		}

		public static DataSet <Tuple1 <Double>> run2(Graph <Long, Long, Double> graph) {
			DataSet <Tuple5 <Long, Long, Long, Long, Double>> edgeInfo = graph
				.groupReduceOnNeighbors(new NeighborsFunctionWithVertexValue <Long, Long, Double,
					Tuple5 <Long, Long, Long, Long, Double>>() {

					@Override
					public void iterateNeighbors(Vertex <Long, Long> vertex,
												 Iterable <Tuple2 <Edge <Long, Double>,
													 Vertex <Long, Long>>> neighbors,
												 Collector <Tuple5 <Long, Long, Long, Long, Double>> out) throws Exception {
						for (Tuple2 <Edge <Long, Double>, Vertex <Long, Long>> neighbor : neighbors) {
							out.collect(Tuple5.of(vertex.f0, vertex.f1, neighbor.f1.f0, neighbor.f1.f1, neighbor.f0.f2));
						}
					}
				}, EdgeDirection.OUT);

			DataSet <Tuple1 <Double>> m = edgeInfo.aggregate(Aggregations.SUM, 4).project(4);

			DataSet <HashMap> inAndOutModularity = edgeInfo
				.mapPartition(
					new MapModularity())
				.reduce(new ReduceFunction <HashMap>() {
					@Override
					public HashMap reduce(HashMap value1,
										  HashMap value2)
						throws Exception {
						HashMap <Long, Tuple2 <Double, Double>> v1 = value1;
						HashMap <Long, Tuple2 <Double, Double>> v2 = value2;
						for (Entry <Long, Tuple2 <Double, Double>> entry : v2.entrySet()) {
							Tuple2 <Double, Double> v1Modu = v1.getOrDefault(entry.getKey(), Tuple2.of(0.0, 0.0));
							v1Modu.f0 += entry.getValue().f0;
							v1Modu.f1 += entry.getValue().f1;
							v1.put(entry.getKey(), v1Modu);
						}
						return value1;
					}
				});

			DataSet <Tuple1 <Double>> modularity = inAndOutModularity
				.map(new RichMapFunction <HashMap, Tuple1 <Double>>() {
					@Override
					public Tuple1 <Double> map(HashMap value) throws Exception {
						double m = ((Tuple1 <Double>) getRuntimeContext().getBroadcastVariable("m").get(0)).f0;
						double in = 0;
						double out = 0;
						HashMap <Long, Tuple2 <Double, Double>> v = value;
						for (Entry <Long, Tuple2 <Double, Double>> entry : v.entrySet()) {
							double localout = 0;
							in += entry.getValue().f0;
							localout = entry.getValue().f0 + entry.getValue().f1;
							out += Math.pow(localout, 2);
						}

						in /= m;

						return Tuple1.of(in - out / Math.pow(m, 2));
					}
				}).withBroadcastSet(m, "m");

			return modularity;
		}

		private static class MapModularity
			extends RichMapPartitionFunction <Tuple5 <Long, Long, Long, Long, Double>, HashMap> {

			@Override
			public void mapPartition(Iterable <Tuple5 <Long, Long, Long, Long, Double>> values,
									 Collector <HashMap> out) throws Exception {

				HashMap <Long, Tuple2 <Double, Double>> inModularitys = new HashMap <>();
				for (Tuple5 <Long, Long, Long, Long, Double> value : values) {

					Tuple2 <Double, Double> inAndOut = inModularitys.getOrDefault(value.f1, Tuple2.of(0.0, 0.0));
					if (value.f1.equals(value.f3)) {
						double inModu = inAndOut.f0;
						inModu += value.f4;
						inAndOut.f0 = inModu;
						inModularitys.put(value.f1, inAndOut);
					} else {
						double outModu = inAndOut.f1;
						outModu += value.f4;
						inAndOut.f1 = outModu;
						inModularitys.put(value.f1, inAndOut);
					}
				}
				out.collect(inModularitys);
			}
		}

		public static DataSet <Tuple1 <Double>> run(Graph <Long, Long, Double> graph) {
			//Save the dense matrix in a DataSet with the form of triad. The three elements are row id, column id and the
			// value.
			//We only need to calculate the matrix through the diag as well as the column, so this design is convenient.

			//change all the edges to the tripe. the three position of the Tuple3 represents the two community of
			// the two nodes of the edge, and the 3rd position is 1
			// may try it with getTriplets().

			//node id, neighbor id, edge weight
			DataSet <Tuple3 <Long, Long, Long>> communityInfo = graph
				.groupReduceOnNeighbors(new ErgodicEdge(), EdgeDirection.OUT);
			//groupby all the edge information and form the k*k matrix
			DataSet <Tuple3 <Long, Long, Long>> communityInfoReduced = communityInfo
				.groupBy(new SelectTuple())
				.reduce(new ReduceOnCommunity());
			//the following two steps calculate m.
			//this step calculate sum on row
			DataSet <Tuple1 <Long>> reducedOnRow = communityInfoReduced
				.groupBy(1)
				.aggregate(Aggregations.SUM, 2)
				.project(2);
			DataSet <Tuple1 <Long>> m = reducedOnRow
				.aggregate(Aggregations.SUM, 0);
			DataSet <Tuple1 <Long>> temp2 = reducedOnRow
				.map(new MapSquare()).aggregate(Aggregations.SUM, 0);
			//.reduce(new Sum());
			DataSet <Tuple1 <Long>> temp1 = communityInfoReduced
				.filter(new FilterDiag())
				.aggregate(Aggregations.SUM, 2)
				.project(2);
			//temp1, temp2 and m are DataSet that only contains one element.
			return temp1
				.cross(temp2)
				.with(new CrossStep())
				.withBroadcastSet(m, "m");
		}

		public static class ErgodicEdge
			implements NeighborsFunctionWithVertexValue <Long, Long, Double, Tuple3 <Long, Long, Long>> {
			private static final long serialVersionUID = 5295386257754049577L;

			@Override
			public void iterateNeighbors(Vertex <Long, Long> vertex,
										 Iterable <Tuple2 <Edge <Long, Double>, Vertex <Long, Long>>> neighbors,
										 Collector <Tuple3 <Long, Long, Long>> out) {
				long f0 = vertex.f1.longValue();
				for (Tuple2 <Edge <Long, Double>, Vertex <Long, Long>> neighbor : neighbors) {
					//long f1 = neighbor.f1.f1.longValue();
					long f1 = neighbor.f1.f0;
					out.collect(Tuple3.of(f0, f1, 1L));
				}
			}
		}

		public static class SelectTuple
			implements KeySelector <Tuple3 <Long, Long, Long>, Tuple2 <Long, Long>> {
			private static final long serialVersionUID = 5638365638596494304L;

			@Override
			public Tuple2 <Long, Long> getKey(Tuple3 <Long, Long, Long> value) throws Exception {
				return Tuple2.of(value.f0, value.f1);
			}
		}

		public static class ReduceOnCommunity
			implements ReduceFunction <Tuple3 <Long, Long, Long>> {
			private static final long serialVersionUID = 3502336992662864358L;

			@Override
			public Tuple3 <Long, Long, Long> reduce(Tuple3 <Long, Long, Long> value1,
													Tuple3 <Long, Long, Long> value2) throws Exception {
				return new Tuple3 <>(value1.f0, value1.f1, value1.f2 + value2.f2);
			}
		}

		public static class MapSquare
			implements MapFunction <Tuple1 <Long>, Tuple1 <Long>> {
			private static final long serialVersionUID = -1719101888137570397L;

			@Override
			public Tuple1 <Long> map(Tuple1 <Long> value) throws Exception {
				return new Tuple1 <>(value.f0 * value.f0);
			}
		}

		public static class FilterDiag implements FilterFunction <Tuple3 <Long, Long, Long>> {
			private static final long serialVersionUID = 6595663411872011784L;

			@Override
			public boolean filter(Tuple3 <Long, Long, Long> value) throws Exception {
				return value.f0.equals(value.f1);
			}
		}
		//
		//	public static class Sum implements ReduceFunction <Tuple1 <Long>> {
		//		private static final long serialVersionUID = -5418729191039529263L;
		//
		//		@Override
		//		public Tuple1 <Long> reduce(Tuple1 <Long> value1, Tuple1 <Long> value2) throws Exception {
		//			return new Tuple1 <>(value1.f0 + value2.f0);
		//		}
		//	}

		protected static class CrossStep extends AbstractRichFunction
			implements CrossFunction <Tuple1 <Long>, Tuple1 <Long>, Tuple1 <Double>> {
			private static final long serialVersionUID = -7359362890112928974L;
			private Tuple1 <Long> mTuple;

			@Override
			public void open(Configuration parameters) throws Exception {
				List <Tuple1 <Long>> dicList = getRuntimeContext().getBroadcastVariable("m");
				for (Tuple1 <Long> s : dicList) {
					mTuple = s;
				}
			}

			@Override
			public Tuple1 <Double> cross(Tuple1 <Long> temp1Tuple, Tuple1 <Long> temp2Tuple) throws Exception {
				long temp1 = temp1Tuple.f0;
				long temp2 = temp2Tuple.f0;
				long m = mTuple.f0;
				return new Tuple1 <>(1. * temp1 / m - 1. * temp2 / (m * m));
			}
		}
	}
}
