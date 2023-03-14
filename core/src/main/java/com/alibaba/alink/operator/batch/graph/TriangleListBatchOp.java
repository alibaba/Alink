package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.library.clustering.directed.TriangleListing;
import org.apache.flink.graph.library.clustering.directed.TriangleListing.Result;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.DoubleValue;
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
import com.alibaba.alink.params.graph.TriangleListParams;

@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES))
@OutputPorts(values = @PortSpec(value = PortType.DATA))
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@NameCn("计数三角形")
@NameEn("Count Triangles")
public class TriangleListBatchOp extends BatchOperator<TriangleListBatchOp>
    implements TriangleListParams<TriangleListBatchOp> {
    private static final long serialVersionUID = -5985547688589472574L;

    public TriangleListBatchOp(Params params) {
        super(params);
    }

    public TriangleListBatchOp() {
        super(new Params());
    }

    @Override
    public TriangleListBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String sourceCol = getEdgeSourceCol();
        String targetCol = getEdgeTargetCol();
        String[] outputCols = new String[] {"node1", "node2", "node3"};
        String[] inputEdgeCols = new String[]{sourceCol, targetCol};
        TypeInformation<?>[] inputTypes = in.getColTypes();
        int vertexColTypeIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), sourceCol);
        TypeInformation vertexType = inputTypes[vertexColTypeIndex];
        DataSet<Row> inputData = GraphUtilsWithString
            .input2json(in, inputEdgeCols, 2, false);
        GraphUtilsWithString map = new GraphUtilsWithString(inputData, vertexType);

        DataSet<Edge<Long, Double>> inData = map.inputType2longEdge(inputData, false);
        Graph<Long, Double, Double> graph = Graph
            .fromDataSet(inData, MLEnvironmentFactory.get(in.getMLEnvironmentId()).getExecutionEnvironment())
            .mapVertices(new MapVertices());


        try {
            DataSet<Tuple3<Long, Long, Long>> resData = TriangleList.run(graph);
            DataSet<Row> resEdges = map.long2outputTriangleList(resData);
            this.setOutput(resEdges, outputCols,
                new TypeInformation<?>[]{inputTypes[vertexColTypeIndex],
                    inputTypes[vertexColTypeIndex], inputTypes[vertexColTypeIndex]});
            return this;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static class MapVertices implements MapFunction<Vertex<Long, NullValue>, Double> {
        private static final long serialVersionUID = 1770812955754922346L;

        @Override
        public Double map(Vertex<Long, NullValue> value) throws Exception {
            return 1.0;
        }
    }

	/**
	 * Return the numbers of triangles of the input graph.
	 */
	public static class TriangleList {
		public static DataSet <Tuple3 <Long, Long, Long>> run(Graph <Long, Double, Double> graph) throws Exception {
			DataSet <TriangleListing.Result <LongValue>> inn = graph
				.translateGraphIds(new LongToLongValue())
				.translateVertexValues(new DoubleToDoubleValue())
				.translateEdgeValues(new DoubleToDoubleValue())
				.run(new TriangleListing <>());
			return inn.flatMap(new FlatMapOut());
		}

		public static class LongToLongValue implements TranslateFunction <Long, LongValue> {
			private static final long serialVersionUID = -8328414272957924783L;

			@Override
			public LongValue translate(Long value, LongValue reuse) {
				return new LongValue(value);
			}
		}

		public static class DoubleToDoubleValue implements TranslateFunction <Double, DoubleValue> {
			private static final long serialVersionUID = -1396593185269273081L;

			@Override
			public DoubleValue translate(Double value, DoubleValue reuse) {
				return new DoubleValue(value);
			}
		}

		public static class FlatMapOut
			implements FlatMapFunction <Result <LongValue>, Tuple3 <Long, Long, Long>> {
			private static final long serialVersionUID = -7744611721605759120L;

			@Override
			public void flatMap(Result <LongValue> value, Collector <Tuple3 <Long, Long, Long>> out) {
				Tuple3 <Long, Long, Long> temp = new Tuple3 <>();
				temp.f0 = value.getVertexId0().getValue();
				temp.f1 = value.getVertexId1().getValue();
				temp.f2 = value.getVertexId2().getValue();
				out.collect(temp);
			}
		}
	}
}
