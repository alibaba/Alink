package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
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
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.CommunityDetectionClassifyBatchOp.CommunityDetection.ClusterLabelMerger;
import com.alibaba.alink.operator.batch.graph.CommunityDetectionClassifyBatchOp.CommunityDetection.ClusterMessageGroupFunction;
import com.alibaba.alink.params.graph.CommunityDetectionClusterParams;
@InputPorts(values = {
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES),
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRAPH_VERTICES, isOptional = true),
})
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "vertexCol", portIndices = 1)
@ParamSelectColumnSpec(name = "vertexWeightCol", portIndices = 1)
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeWeightCol", portIndices = 0)
@NameCn("标签传播聚类")
@NameEn("Common Detection Cluster")
public class CommunityDetectionClusterBatchOp extends BatchOperator <CommunityDetectionClusterBatchOp>
	implements CommunityDetectionClusterParams <CommunityDetectionClusterBatchOp> {

	private static final long serialVersionUID = 2041752348271477963L;

	public CommunityDetectionClusterBatchOp(Params params) {
		super(params);
	}

	public CommunityDetectionClusterBatchOp() {
		super(new Params());
	}

	@Override
	public CommunityDetectionClusterBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkMinOpSize(1, inputs);
		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();
		//the value of edges mean the relationship between vertices.
		String edgeWeightCol = getEdgeWeightCol();
		String[] outputCols = new String[] {"vertex", "label"};
		Integer maxIter = getMaxIter();

		Boolean directed = getAsUndirectedGraph();
		boolean hasEdgeWeightCol = !(edgeWeightCol == null || edgeWeightCol == "null");

		BatchOperator edgesBatch = inputs[0];
		String[] inputEdgeCols = hasEdgeWeightCol ? new String[] {sourceCol, targetCol, edgeWeightCol}
			: new String[] {sourceCol, targetCol};
		TypeInformation[] edgeTypes = TableUtil.findColTypes(edgesBatch.getSchema(), inputEdgeCols);
		if (!edgeTypes[0].equals(edgeTypes[1])) {
			throw new RuntimeException(String.format(
				"Edge input data, sourceCol and targetCol should be same type, sourceCol type %s, targetCol type %s",
				edgeTypes[0], edgeTypes[1]));
		}
		if (!edgeTypes[0].equals(Types.STRING) && !edgeTypes[0].equals(Types.LONG) && !edgeTypes[0].equals(Types.INT)) {
			throw new RuntimeException(String.format(
				"Edge input data, sourceCol and targetCol should be string, long or integer. Input type is %s",
				edgeTypes[0]));
		}
		DataSet <Row> edgeRows = edgesBatch.select(inputEdgeCols).getDataSet();
		DataSet <Edge <Long, Float>> edges;
		DataSet <Tuple2 <Long, Float>> vertex = null;
		DataSet <Tuple2 <String, Long>> nodeMapping;
		if (inputs.length == 2) {
			BatchOperator verticesBatch = inputs[1];
			String vertexCol = getVertexCol();
			String vertexWeightCol = getVertexWeightCol();
			boolean hasVertexWeightCol = !(vertexWeightCol == null || vertexWeightCol == "null");
			String[] inputVertexCols = hasVertexWeightCol ? new String[] {vertexCol, vertexWeightCol}
				: new String[] {vertexCol};
			TypeInformation vertexType = TableUtil.findColType(verticesBatch.getSchema(), vertexCol);
			if (!vertexType.equals(edgeTypes[0])) {
				throw new RuntimeException(String.format(
					"Edge sourceCol and Vertex column should be same type, sourceCol type %s, Vertex column type %s",
					edgeTypes[0], vertexType));
			}
			DataSet <Row> vertexRows = verticesBatch.select(inputVertexCols).getDataSet();
			nodeMapping = GraphUtils.graphNodeIdMapping(edgeRows, new int[] {0, 1}, vertexRows, 0);
			vertexRows = GraphUtils.mapOriginalToId(vertexRows, nodeMapping, new int[] {0});
			vertex = vertexRows.map(new MapFunction <Row, Tuple2 <Long, Float>>() {
				@Override
				public Tuple2 <Long, Float> map(Row value) throws Exception {
					float weight = 1.0F;
					if (hasVertexWeightCol) {
						weight = ((Number) value.getField(1)).floatValue();
					}
					return Tuple2.of((Long) value.getField(0), weight);
				}
			}).name("vertexRows_map_vertex_weight");
		} else {
			nodeMapping = GraphUtils.graphNodeIdMapping(edgeRows, new int[] {0, 1}, null, 0);
		}
		edgeRows = GraphUtils.mapOriginalToId(edgeRows, nodeMapping, new int[] {0, 1});

		edges = edgeRows.flatMap(new FlatMapFunction <Row, Edge <Long, Float>>() {
			@Override
			public void flatMap(Row value, Collector <Edge <Long, Float>> out) throws Exception {
				float weight = 1.0F;
				if (hasEdgeWeightCol) {
					weight = Float.valueOf(String.valueOf(value.getField(2)));
				}
				out.collect(new Edge <>((Long) value.getField(0), (Long) value.getField(1), weight));
				if (directed) {
					out.collect(new Edge <>((Long) value.getField(1), (Long) value.getField(0), weight));
				}
			}
		}).name("map_row_to_edge");

		DataSet <Tuple3 <Long, Long, Float>> initVertexLabel = nodeMapping.map(
			new MapFunction <Tuple2 <String, Long>, Tuple3 <Long, Long, Float>>() {
				@Override
				public Tuple3 <Long, Long, Float> map(Tuple2 <String, Long> value) throws Exception {
					return Tuple3.of(value.f1, value.f1, 1.0F);
				}
			}).name("init_vertex_label_map");

		if (inputs.length == 2) {
			initVertexLabel = initVertexLabel.leftOuterJoin(vertex).where(0).equalTo(0)
				.with(
					new JoinFunction <Tuple3 <Long, Long, Float>, Tuple2 <Long, Float>, Tuple3 <Long, Long, Float>>() {
						@Override
						public Tuple3 <Long, Long, Float> join(Tuple3 <Long, Long, Float> first,
															   Tuple2 <Long, Float> second) throws Exception {
							if (null == second) {
								return first;
							} else {
								return Tuple3.of(first.f0, first.f1, second.f1);
							}
						}
					}).name("join_origin_vertex_input_weight");
		}

		IterativeDataSet <Tuple3 <Long, Long, Float>> iteration = initVertexLabel.iterate(maxIter);
		iteration.name("delta_iteration");

		DataSet <Tuple3 <Long, Long, Float>> messageDataSet = edges.join(iteration)
			.where(0).equalTo(0).with(
				new JoinFunction <Edge <Long, Float>, Tuple3 <Long, Long, Float>, Tuple3 <Long, Long,
					Float>>() {
					@Override
					public Tuple3 <Long, Long, Float> join(Edge <Long, Float> first,
														   Tuple3 <Long, Long, Float> second)
						throws Exception {
						return Tuple3.of(first.f1, second.f1, first.f2 * second.f2);
					}
				}).name("join_send_messages");

		DataSet <Tuple3 <Long, Long, Integer>> currentLabel = messageDataSet.groupBy(0)
			.reduceGroup(new ClusterMessageGroupFunction()).name("message_group_function");
		DataSet <Tuple4 <Long, Long, Float, Boolean>> changeVertexLabel = currentLabel.rightOuterJoin(iteration)
			.where(0).equalTo(0).with(new ClusterLabelMerger()).name("join_label_merge");
		DataSet <Tuple1 <Long>> vertexUpdateStatus = changeVertexLabel.filter(
			new FilterFunction <Tuple4 <Long, Long, Float, Boolean>>() {
				@Override
				public boolean filter(Tuple4 <Long, Long, Float, Boolean> value) throws Exception {
					return value.f3;
				}
			}).project(0);
		DataSet <Tuple3 <Long, Long, Float>> udpateVertexLabel = changeVertexLabel.project(0, 1, 2);
		DataSet <Row> finalVertexLabel = iteration.closeWith(udpateVertexLabel, vertexUpdateStatus).map(
			new MapFunction <Tuple3 <Long, Long, Float>, Row>() {
				@Override
				public Row map(Tuple3 <Long, Long, Float> value) throws Exception {
					Row row = new Row(2);
					row.setField(0, value.f0);
					row.setField(1, value.f1);
					return row;
				}
			});

		DataSet <Row> res = GraphUtils.mapIdToOriginal(finalVertexLabel, nodeMapping, new int[] {0}, edgeTypes[0]);
		this.setOutput(res, outputCols, new TypeInformation <?>[] {edgeTypes[0], Types.LONG});
		return this;
	}

}
