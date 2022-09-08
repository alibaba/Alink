package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.graph.GraphUtils;
import com.alibaba.alink.params.graph.ModularityCalParams;
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

}
