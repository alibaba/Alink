package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.ml.api.misc.param.Params;
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
import com.alibaba.alink.operator.batch.utils.GraphTransformUtils;
import com.alibaba.alink.operator.common.graph.GraphUtilsWithString;
import com.alibaba.alink.params.graph.TreeDepthParams;
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES))
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeWeightCol", portIndices = 0)
@NameCn("树深度")
public class TreeDepthBatchOp extends BatchOperator <TreeDepthBatchOp> implements TreeDepthParams <TreeDepthBatchOp> {
	private static final long serialVersionUID = -6574485904046547006L;

	public TreeDepthBatchOp(Params params) {
		super(params);
	}

	public TreeDepthBatchOp() {
		super(new Params());
	}

	@Override
	public TreeDepthBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();
		String weightCol = getEdgeWeightCol();
		boolean hasWeightCol = !(weightCol == null);
		String[] outputCols = new String[]{"vertices", "root", "treeDepth"};
		Integer maxIter = getMaxIter();
		String[] inputEdgeCols = hasWeightCol?
			new String[] {sourceCol, targetCol, weightCol} : new String[] {sourceCol, targetCol};
		TypeInformation <?>[] inputTypes = in.getColTypes();
		int vertexColTypeIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), sourceCol);
		TypeInformation vertexType = inputTypes[vertexColTypeIndex];
		DataSet<Row> inputData = GraphUtilsWithString.input2json(in, inputEdgeCols,
			2, true);
		GraphUtilsWithString map = new GraphUtilsWithString(inputData, vertexType);
		DataSet <Edge <Long, Double>> inData = map.inputType2longEdge(inputData, true);
		Graph <Long, Double, Double> graph = Graph.fromDataSet(inData,
			MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment())
			.mapVertices(new GraphTransformUtils.MapVerticesTreeDepth());

		DataSet <Row> resEdges = map.long2outputTreeDepth(new TreeDepth(maxIter).run(graph));

		this.setOutput(resEdges, outputCols,
			new TypeInformation <?>[] {vertexType, vertexType, Types.DOUBLE});
		return this;

	}
}
