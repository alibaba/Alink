package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.Row;

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
import com.alibaba.alink.params.graph.ConnectedComponentParams;
import com.alibaba.alink.params.graph.HasSetStable;

@InputPorts(values = {
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES),
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRAPH_VERTICES, isOptional = true),
})
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "vertexCol", portIndices = 1)
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@NameCn("最大联通分量")
@NameEn("ConnectedComponents")
public class ConnectedComponentsBatchOp extends BatchOperator <ConnectedComponentsBatchOp>
	implements ConnectedComponentParams <ConnectedComponentsBatchOp> {
	private static final long serialVersionUID = -7920188555691775911L;

	public ConnectedComponentsBatchOp(Params params) {
		super(params);
	}

	public ConnectedComponentsBatchOp() {
		super(new Params());
	}

	@Override
	public ConnectedComponentsBatchOp linkFrom(BatchOperator <?>... inputs) {

		GraphUtilsWithString map;
		Graph <Long, Double, Double> graph;

		BatchOperator edgeBatch = inputs[0];
		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();

		String outIdCol = "node";
		String groupIdCol = "groupId";
		Integer maxIter = getMaxIter();
		String[] inputEdgeCols = new String[] {sourceCol, targetCol};
		TypeInformation <?>[] inputTypes = edgeBatch.getColTypes();
		int vertexColTypeIndex = TableUtil.findColIndexWithAssertAndHint(edgeBatch.getColNames(), sourceCol);
		TypeInformation vertexType = inputTypes[vertexColTypeIndex];
		DataSet<Row> inputEdgeData = GraphUtilsWithString.input2json(edgeBatch, inputEdgeCols, 2, false);

		ExecutionEnvironment env = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment();
		int inputSize = inputs.length;
		if (inputSize == 2) {
			BatchOperator vertexBatch = inputs[1];
			String vertexCol = getVertexCol();
			DataSet<Row> inputVertexData = GraphUtilsWithString
				.input2json(vertexBatch, new String[] {vertexCol}, 1, false);
			map = new GraphUtilsWithString(inputEdgeData, inputVertexData, vertexType, getParams().get(HasSetStable.SET_STABLE));
			DataSet <Edge <Long, Double>> edges = map
				.inputType2longEdge(inputEdgeData, false);
			DataSet<Vertex<Long, Double>> vertices = map.transformInputVertexWithoutWeight(inputVertexData);
			graph = Graph.fromDataSet(vertices, edges, env);
		} else {
			map = new GraphUtilsWithString(inputEdgeData, vertexType, getParams().get(HasSetStable.SET_STABLE));
			DataSet <Edge <Long, Double>> edges = map
				.inputType2longEdge(inputEdgeData, false);
			graph = Graph.fromDataSet(edges, env)
				.mapVertices(new MapVertices());
		}
		//donot need to get undirected, because gelly does it.

		try {
			DataSet <Row> res = map.double2outputTypeVertex(
				new GSAConnectedComponents <Long, Double, Double>(maxIter).run(graph), Types.LONG);
			this.setOutput(res, new String[] {outIdCol, groupIdCol},
				new TypeInformation <?>[] {vertexType, Types.LONG});
			return this;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static class MapVertices implements MapFunction <Vertex <Long, NullValue>, Double> {

		private static final long serialVersionUID = 6332483212498770260L;

		@Override
		public Double map(Vertex <Long, NullValue> value) throws Exception {
			return Double.valueOf(value.f0);
		}
	}

}
