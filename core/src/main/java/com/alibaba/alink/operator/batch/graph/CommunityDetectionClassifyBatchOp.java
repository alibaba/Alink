package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.CommunityDetection.ClassifyLabelMerger;
import com.alibaba.alink.operator.batch.graph.CommunityDetection.ClassifyMessageGroupFunction;
import com.alibaba.alink.operator.common.graph.GraphUtils;
import com.alibaba.alink.params.graph.CommunityDetectionClassifyParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@InputPorts(values = {
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES),
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRAPH_VERTICES),
})
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "vertexCol", portIndices = 1)
@ParamSelectColumnSpec(name = "vertexLabelCol", portIndices = 1)
@ParamSelectColumnSpec(name = "vertexWeightCol", portIndices = 1,
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeWeightCol", portIndices = 0, allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("标签传播分类")
public class CommunityDetectionClassifyBatchOp extends BatchOperator <CommunityDetectionClassifyBatchOp>
	implements CommunityDetectionClassifyParams <CommunityDetectionClassifyBatchOp> {
	private static final long serialVersionUID = -2264855900960878969L;

	public CommunityDetectionClassifyBatchOp(Params params) {
		super(params);
	}

	public CommunityDetectionClassifyBatchOp() {
		super(new Params());
	}

	//the input data are two batchOp, the edges and some labelled vertices.
	@Override
	public CommunityDetectionClassifyBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();
		//the value of edges mean the relationship between vertices.
		String edgeWeightCol = getEdgeWeightCol();
		String[] outputCols = new String[] {"vertex", "label"};
		Integer maxIter = getMaxIter();
		Double delta = getDelta();
		String vertexCol = getVertexCol();
		String vertexLabelCol = getVertexLabelCol();
		String vertexWeightCol = getVertexWeightCol();
		Boolean directed = getAsUndirectedGraph();
		boolean hasEdgeWeightCol = !(edgeWeightCol == null || edgeWeightCol == "null");
		boolean hasVertexWeightCol = !(vertexWeightCol == null || vertexWeightCol == "null");
		BatchOperator edgesBatch = inputs[0];
		BatchOperator verticesBatch = inputs[1];
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
		String[] inputVertexCols = hasVertexWeightCol ? new String[] {vertexCol, vertexLabelCol, vertexWeightCol}
			: new String[] {vertexCol, vertexLabelCol};
		TypeInformation vertexType = TableUtil.findColType(verticesBatch.getSchema(), vertexCol);
		TypeInformation labelType = TableUtil.findColType(verticesBatch.getSchema(), vertexLabelCol);
		if (!vertexType.equals(edgeTypes[0])) {
			throw new RuntimeException(String.format(
				"Edge sourceCol and Vertex column should be same type, sourceCol type %s, Vertex column type %s",
				edgeTypes[0], vertexType));
		}

		DataSet <Row> edgeRows = edgesBatch.select(inputEdgeCols).getDataSet();
		DataSet <Row> vertexRows = verticesBatch.select(inputVertexCols).getDataSet();
		DataSet <Tuple2 <String, Long>> labelMapping = GraphUtils.graphNodeIdMapping(vertexRows,
			new int[] {1}, null, 0);
		DataSet <Tuple2 <String, Long>> nodeMapping = GraphUtils.graphNodeIdMapping(edgeRows,
			new int[] {0, 1}, vertexRows, 0);
		edgeRows = GraphUtils.mapOriginalToId(edgeRows, nodeMapping, new int[] {0, 1});
		// mapping vertex node and label
		vertexRows = GraphUtils.mapOriginalToId(vertexRows, nodeMapping, new int[] {0});
		vertexRows = GraphUtils.mapOriginalToId(vertexRows, labelMapping, new int[] {1});

		// flatMap edges and normalize edges
		DataSet <Edge <Long, Float>> edges = edgeRows.flatMap(new FlatMapFunction <Row, Edge <Long, Float>>() {
			@Override
			public void flatMap(Row value, Collector <Edge <Long, Float>> out) throws Exception {
				Long source = (Long) value.getField(0);
				Long target = (Long) value.getField(1);
				float weight = 1.0F;
				if (hasEdgeWeightCol) {
					weight = Float.valueOf(String.valueOf(value.getField(2)));
				}
				out.collect(new Edge <>(source, target, weight));
				if (directed) {
					out.collect(new Edge <>(target, source, weight));
				}
			}
		}).name("generate_origin_edges_dataset")
			.groupBy(0).reduceGroup(new GroupReduceFunction <Edge <Long, Float>, Edge <Long, Float>>() {
				@Override
				public void reduce(Iterable <Edge <Long, Float>> values, Collector <Edge <Long, Float>> out)
					throws Exception {
					Long sourceNode = 0L;
					HashMap <Long, Float> edgesWeights = new HashMap <>();
					Float totalWeight = 0.F;
					for (Edge <Long, Float> value : values) {
						sourceNode = value.f0;
						totalWeight += value.f2;
						edgesWeights.put(value.f1, edgesWeights.getOrDefault(value.f1, 0.F) + value.f2);
					}
					for (Long node : edgesWeights.keySet()) {
						out.collect(new Edge <>(sourceNode, node, edgesWeights.get(node) / totalWeight));
					}
				}
			}).name("generate_origin_edges_normalize");

		DataSet <Tuple4 <Long, Integer, Float, Boolean>> inputVertexLabel = vertexRows.map(
			new MapFunction <Row, Tuple4 <Long, Integer, Float, Boolean>>() {
				@Override
				public Tuple4 <Long, Integer, Float, Boolean> map(Row value) throws Exception {
					Long node = (Long) value.getField(0);
					Long label = (Long) value.getField(1);
					float weight = 1.0F;
					if (hasVertexWeightCol) {
						weight = Float.valueOf(String.valueOf(value.getField(2)));
					}
					return Tuple4.of(node, label.intValue(), weight, true);
				}
			});

		DataSet <Tuple4 <Long, Integer, Float, Boolean>> allVertexLabel = nodeMapping.leftOuterJoin(inputVertexLabel)
			.where(1).equalTo(0).with(
				new JoinFunction <Tuple2 <String, Long>, Tuple4 <Long, Integer, Float, Boolean>, Tuple4 <Long, Integer
					, Float, Boolean>>() {
					@Override
					public Tuple4 <Long, Integer, Float, Boolean> join(Tuple2 <String, Long> first,
																	   Tuple4 <Long, Integer, Float, Boolean> second)
						throws Exception {
						if (null == second) {
							return Tuple4.of(first.f1, -1, 0.0F, false);
						} else {
							return second;
						}
					}
				}).name("allVertexLabel_join_inputVertexLabel");

		IterativeDataSet <Tuple4 <Long, Integer, Float, Boolean>> iterate = allVertexLabel.iterate(maxIter);

		DataSet <Tuple3 <Long, Integer, Float>> edgesWithVertexLabel = edges.join(iterate)
			.where(0).equalTo(0).with(
				new JoinFunction <Edge <Long, Float>, Tuple4 <Long, Integer, Float, Boolean>, Tuple3 <Long, Integer, Float>>() {
					@Override
					public Tuple3 <Long, Integer, Float> join(Edge <Long, Float> first,
															  Tuple4 <Long, Integer, Float, Boolean> second)
						throws Exception {
						return Tuple3.of(first.f1, second.f1, first.f2 * second.f2);
					}
				}).name("join_send_messages");

		DataSet <Tuple3 <Long, Integer, Float>> currentLabel =
			edgesWithVertexLabel.groupBy(0)
				.reduceGroup(new ClassifyMessageGroupFunction()).name("message_group_function");

		DataSet <Tuple5 <Long, Integer, Float, Boolean, Boolean>> newVertexLabelStatus = currentLabel.rightOuterJoin(
			iterate)
			.where(0).equalTo(0).with(new ClassifyLabelMerger(delta)).name("join_label_merge");
		DataSet <Tuple1 <Long>> terminatedData = newVertexLabelStatus.filter(
			new FilterFunction <Tuple5 <Long, Integer, Float, Boolean, Boolean>>() {
				@Override
				public boolean filter(Tuple5 <Long, Integer, Float, Boolean, Boolean> value) throws Exception {
					return value.f4;
				}
			}).project(0);
		DataSet <Tuple4 <Long, Integer, Float, Boolean>> newVertexLabel = newVertexLabelStatus.project(0, 1, 2, 3);
		DataSet <Row> finalVertexLabel = iterate.closeWith(newVertexLabel, terminatedData).map(
			new MapFunction <Tuple4 <Long, Integer, Float, Boolean>, Row>() {
				@Override
				public Row map(Tuple4 <Long, Integer, Float, Boolean> value) throws Exception {
					Row row = new Row(2);
					row.setField(0, value.f0);
					row.setField(1, value.f1.longValue());
					return row;
				}
			});

		DataSet <Row> res = GraphUtils.mapIdToOriginal(finalVertexLabel, nodeMapping, new int[] {0},
			edgeTypes[0])
			.flatMap(new RichFlatMapFunction <Row, Row>() {

				Map <Long, String> labelMap;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					List <Tuple2 <String, Long>> list = this.getRuntimeContext().getBroadcastVariable("labelDict");
					labelMap = new HashMap <>(list.size());
					for (Tuple2 <String, Long> v : list) {
						labelMap.put(v.f1, v.f0);
					}
				}

				@Override
				public void flatMap(Row value, Collector <Row> out) throws Exception {
					Long classIndex = (Long) value.getField(1);
					if (labelMap.containsKey(classIndex)) {
						value.setField(1, JsonConverter.fromJson(labelMap.get(classIndex), labelType.getTypeClass()));
						out.collect(value);
					}
				}
			}).withBroadcastSet(labelMapping, "labelDict")
			.name("mapping_class_label");

		this.setOutput(res, outputCols, new TypeInformation <?>[] {edgeTypes[0], labelType});
		return this;
	}
}
