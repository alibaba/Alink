package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.library.clustering.undirected.TriangleListing;
import org.apache.flink.graph.library.clustering.undirected.TriangleListing.Result;
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
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.graph.VertexClusterCoefficientParams;

@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES))
@OutputPorts(values = @PortSpec(value = PortType.DATA))
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@NameCn("点聚类系数")
@NameEn("Vertex Cluster Coefficient")
public class VertexClusterCoefficientBatchOp extends BatchOperator <VertexClusterCoefficientBatchOp>
	implements VertexClusterCoefficientParams <VertexClusterCoefficientBatchOp> {
	private static final long serialVersionUID = 3694935054423399372L;

	public VertexClusterCoefficientBatchOp(Params params) {
		super(params);
	}

	public VertexClusterCoefficientBatchOp() {
		super(new Params());
	}

	@Override
	public VertexClusterCoefficientBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();
		String[] outputCols = new String[] {"vertexId", "vertexDegree", "edgeNum", "coefficient"};
		Boolean directed = getAsUndirectedGraph();
		String[] inputEdgeCols = new String[] {sourceCol, targetCol};
		TypeInformation <?>[] inputTypes = in.getColTypes();
		int vertexColTypeIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), sourceCol);
		TypeInformation vertexType = inputTypes[vertexColTypeIndex];
		DataSet<Row> inputData = GraphUtilsWithString
			.input2json(in, inputEdgeCols, 2, false);
		GraphUtilsWithString map = new GraphUtilsWithString(inputData, vertexType);
		DataSet <Edge <Long, Double>> edges = map.inputType2longEdge(inputData, false);

		Graph <Long, Double, Double> graph;
		if (directed) {
			graph = Graph.fromDataSet(edges, MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment())
				.mapVertices(new MapVertices()).getUndirected();
		} else {
			graph = Graph.fromDataSet(edges, MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment())
				.mapVertices(new MapVertices());
		}

		try {
			DataSet <Row> res = map.long2outputVCC(new VertexClusterCoefficient().run(graph));
			this.setOutput(res, outputCols,
				new TypeInformation <?>[] {vertexType, Types.LONG, Types.LONG, Types.DOUBLE});
			return this;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static class MapVertices implements MapFunction <Vertex <Long, NullValue>, Double> {
		private static final long serialVersionUID = -4911075721452544667L;

		@Override
		public Double map(Vertex <Long, NullValue> value) throws Exception {
			return 1.;
		}
	}

	/**
	 * as for each vertex, return 1. its degree 2. the number of triangles with it as a vertex
	 *
	 */

	public static class VertexClusterCoefficient {
		public DataSet <Tuple4 <Long, Long, Long, Double>> run(Graph <Long, Double, Double> graph) throws Exception {
			//get all the triangles and write all the vertices id in Tuple2 position 0. We will then calculate
			// how many times the vertices appear in Tuplw2 position 1 in the following operation.
			DataSet <Tuple2 <Long, Long>> triangleVertex = graph.translateGraphIds(new LongToLongvalue())
				.translateVertexValues(new DoubleToLongvalue())
				.translateEdgeValues(new DoubleToLongvalue())
				.run(new TriangleListing <LongValue, LongValue, LongValue>())
				.flatMap(new Result2Long());
			DataSet <Tuple2 <Long, Long>> vertexCounted = triangleVertex
				.groupBy(0).aggregate(Aggregations.SUM, 1);
			//.reduceGroup(new CountVertex());
			//这个coGroup配对的两个DataSet规模是点的数目。。
			//采用编码解码来优化？？
			return graph.inDegrees()
				.coGroup(vertexCounted)
				.where(0)
				.equalTo(0)
				.with(new CoGroupStep());
		}

		public static class LongToLongvalue implements TranslateFunction <Long, LongValue> {
			private static final long serialVersionUID = 6836903282078114665L;

			@Override
			public LongValue translate(Long value, LongValue reuse) {
				return new LongValue(value);
			}
		}

		public static class DoubleToLongvalue implements TranslateFunction <Double, LongValue> {
			private static final long serialVersionUID = -1849025103879518660L;

			@Override
			public LongValue translate(Double value, LongValue reuse) {
				return new LongValue(value.intValue());
			}
		}

		public static class Result2Long
			implements FlatMapFunction <Result <LongValue>, Tuple2 <Long, Long>> {
			private static final long serialVersionUID = 2997438245762067649L;

			@Override
			public void flatMap(
				Result <LongValue> value,
				Collector <Tuple2 <Long, Long>> out) {
				out.collect(new Tuple2 <>(value.getVertexId0().getValue(), 1L));
				out.collect(new Tuple2 <>(value.getVertexId1().getValue(), 1L));
				out.collect(new Tuple2 <>(value.getVertexId2().getValue(), 1L));
			}
		}

		//    public static class CountVertex
		//            implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		//		private static final long serialVersionUID = -659852844684797387L;
		//
		//		@Override
		//        public void reduce(Iterable<Tuple2<Long, Long>> values,
		//                           Collector<Tuple2<Long, Long>> out) throws Exception {
		//            long id = -1L;
		//            long count = 0L;
		//            for (Tuple2<Long, Long> i : values) {
		//                id = i.f0;
		//                count += 1L;
		//            }
		//            out.collect(new Tuple2<>(id, count));
		//        }
		//    }

		public static class CoGroupStep implements CoGroupFunction <Tuple2 <Long, LongValue>,
			Tuple2 <Long, Long>,
			Tuple4 <Long, Long, Long, Double>> {
			private static final long serialVersionUID = -6391324728861498560L;

			@Override
			public void coGroup(Iterable <Tuple2 <Long, LongValue>> first,
								Iterable <Tuple2 <Long, Long>> second,
								Collector <Tuple4 <Long, Long, Long, Double>> out) {
				for (Tuple2 <Long, Long> i : second) {
					Tuple4 <Long, Long, Long, Double> outSingle = new Tuple4 <>();
					Tuple2 <Long, LongValue> firstSingle = first.iterator().next();
					outSingle.f0 = firstSingle.f0;
					outSingle.f1 = firstSingle.f1.getValue();
					outSingle.f2 = i.f1;
					outSingle.f3 = outSingle.f2 * 2. / (outSingle.f1 * (outSingle.f1 - 1));
					out.collect(outSingle);
				}
			}
		}
	}
}
