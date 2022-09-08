package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.graph.GraphUtilsWithString;
import com.alibaba.alink.params.graph.VertexClusterCoefficientParams;

@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES))
@OutputPorts(values = @PortSpec(value = PortType.DATA))
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@NameCn("点聚类系数")
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
}
