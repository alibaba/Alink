package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

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
import com.alibaba.alink.params.graph.MultiSourceShortestPathParams;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Multi Source Shortest Path
 * given some source points, compute shortest path to the source points
 * output root node and nodes in shortest path
 */
@InputPorts(values = {
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES),
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRAPH_VERTICES),
})
@OutputPorts(values = @PortSpec(value = PortType.DATA))
@ParamSelectColumnSpec(name = "sourcePointCol", portIndices = 1)
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeWeightCol", portIndices = 0)
@NameCn("多源最短路径")
public class MultiSourceShortestPathBatchOp extends BatchOperator <MultiSourceShortestPathBatchOp>
	implements MultiSourceShortestPathParams <MultiSourceShortestPathBatchOp> {
	private static final long serialVersionUID = -1637471953684406867L;

	public MultiSourceShortestPathBatchOp(Params params) {
		super(params);
	}

	public MultiSourceShortestPathBatchOp() {
		super(new Params());
	}

	@Override
	public MultiSourceShortestPathBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);
		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();

		String sourcePointCol = getSourcePointCol();
		String[] outputCols = new String[] {"vertex", "root_node", "node_list", "distance"};
		Integer maxIter = getMaxIter();
		Boolean directed = getAsUndirectedGraph();

		String weightCol = getEdgeWeightCol();
		boolean hasWeightCol = weightCol != null;
		String[] inputEdgeCols = hasWeightCol ? new String[] {sourceCol, targetCol, weightCol} :
			 new String[] {sourceCol, targetCol};

		BatchOperator edgesBatch = inputs[0];
		TypeInformation <?> vertexType = TableUtil.findColTypeWithAssertAndHint(edgesBatch.getSchema(), sourceCol);
		DataSet<Row> inputData = GraphUtilsWithString.input2json(edgesBatch, inputEdgeCols, 2, true);
		GraphUtilsWithString map = new GraphUtilsWithString(inputData, vertexType);
		DataSet <Edge <Long, Double>> edges = map
			.inputType2longEdge(inputData, hasWeightCol);

		BatchOperator verticesBatch = inputs[1];
		String[] inputVertexCols = new String[]{sourcePointCol};
		DataSet<Row> inputVertexData = GraphUtilsWithString.input2json(verticesBatch, inputVertexCols, 1, true);
		DataSet<Vertex<Long, Double>> sourceVertices = map.transformInputVertexWithoutWeight(inputVertexData);

		Graph <Long, Tuple3<Long, Long, Double>, Double> graphInitial = Graph
			.fromDataSet(edges, MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment())
			.mapVertices(new MapVertices());
		DataSet <Vertex <Long, Tuple3<Long, Long, Double>>> vertices = graphInitial.getVertices()
			.leftOuterJoin(sourceVertices).where(0).equalTo(0)
			.with(new JoinFunction <Vertex <Long, Tuple3<Long, Long, Double>>, Vertex <Long, Double>, Vertex <Long, Tuple3<Long, Long, Double>>>() {
				private static final long serialVersionUID = 1964647721649366980L;

				@Override
				public Vertex <Long, Tuple3<Long, Long, Double>> join(Vertex <Long, Tuple3<Long, Long, Double>> first, Vertex <Long, Double> second) throws Exception {
					if (second != null) {
						Vertex<Long, Tuple3<Long, Long, Double>> r = new Vertex<>();
						r.setId(first.f0);
						r.setValue(Tuple3.of(first.f0, first.f0, 0.));
						return r;
					}
					return first;
				}
			});

		Graph <Long, Tuple3<Long, Long, Double>, Double> graph;
		if (directed) {
			graph = Graph.fromDataSet(vertices, edges,
				MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment())
				.getUndirected();
		} else {
			graph = Graph.fromDataSet(vertices, edges,
				MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment());
		}

		DataSet <Vertex <Long, Tuple3<Long, Long, Double>>> resData = graph
			.runScatterGatherIteration(new MessengerSendFunction(), new VertexUpdater(), maxIter)
			.getVertices();
		DataSet<Tuple4 <Long, Long, Long, Double>> resultVertex = resData.map(
			new MapFunction <Vertex <Long, Tuple3 <Long, Long, Double>>, Tuple4<Long, Long, Long, Double>>() {
				@Override
				public Tuple4<Long, Long, Long, Double> map(Vertex <Long, Tuple3 <Long, Long, Double>> value) throws Exception {
					return Tuple4.of(value.f0, value.f1.f0, value.f1.f1, value.f1.f2);
			}
		});

		DataSet <Row> stringVertex = map.long2StringSSSP(resultVertex);
		DataSet<Row> noSourceVertex = stringVertex.filter(new FilterFunction <Row>() {
			@Override
			public boolean filter(Row row) throws Exception {
				return row.getField(1) == null;
			}
		}).map(new MapFunction <Row, Row>() {
			@Override
			public Row map(Row row) throws Exception {
				row.setField(2, "");
				row.setField(3, -1.0);
				return row;
			}
		});
		DataSet<Row> resultStringVertex = stringVertex
			.filter(new FilterFunction <Row>() {
				@Override
				public boolean filter(Row row) throws Exception {
					return row.getField(1) != null;
				}
			})
			.groupBy(
			new KeySelector <Row, String>() {
				@Override
				public String getKey(Row row) throws Exception {
					return String.valueOf(row.getField(1));
				}
			}).reduceGroup(
			new GroupReduceFunction <Row, Row>() {
				@Override
				public void reduce(Iterable <Row> iterable, Collector <Row> collector) throws Exception {
					HashMap<Object, Object> relationMap = new HashMap<>();
					HashMap<Object, Double> leafPathValue = new HashMap<>();
					HashSet<Object> parentPointSet = new HashSet<>();
					Object root = null;
					Object son = null;
					Object parent = null;
					for (Row row : iterable) {
						root = row.getField(1);
						son = row.getField(0);
						parent = row.getField(2);
						parentPointSet.add(parent);
						relationMap.put(son, parent);
						leafPathValue.put(son, (Double) row.getField(3));
					}
					ArrayList<Object> leafPoint = new ArrayList <>();
					relationMap.forEach((key, value) -> {
						if (!parentPointSet.contains(key)) {
							leafPoint.add(key);
						}
					});
					HashSet<Object> outputPoint = new HashSet<>();
					for (Object leaf : leafPoint) {
						ArrayList<Object> pathNodes = new ArrayList <>();
						pathNodes.add(leaf);
						parent = relationMap.get(leaf);
						pathNodes.add(parent);
						while (!parent.equals(root)) {
							parent = relationMap.get(parent);
							pathNodes.add(parent);
						}
						ArrayList<String> pathNodeNames = new ArrayList <>();
						pathNodes.forEach(node -> pathNodeNames.add(String.valueOf(node)));
						for (int i = 0; i < pathNodes.size(); i++) {
							Object currentNode = pathNodes.get(i);
							if (outputPoint.contains(currentNode)) {
								break;
							}
							Row result = new Row(4);
							result.setField(0, currentNode);
							result.setField(1, root);
							result.setField(2, StringUtils.join(pathNodeNames.subList(i, pathNodeNames.size()), ","));
							result.setField(3, leafPathValue.get(currentNode));
							collector.collect(result);
							outputPoint.add(currentNode);
						}
					}
					if (!outputPoint.contains(root)) {
						Row result = new Row(4);
						result.setField(0, root);
						result.setField(1, root);
						result.setField(2, String.valueOf(root));
						result.setField(3, leafPathValue.get(root));
						collector.collect(result);
					}
				}
			});

		DataSet<Row> result = resultStringVertex.union(noSourceVertex);
		this.setOutput(result, outputCols, new TypeInformation <?>[] {vertexType, vertexType, Types.STRING, Types.DOUBLE});
		return this;
	}

	public static class MapVertices implements MapFunction <Vertex <Long, NullValue>, Tuple3<Long, Long, Double>> {
		private static final long serialVersionUID = -6624679629933017172L;

		@Override
		public Tuple3<Long, Long, Double> map(Vertex <Long, NullValue> value) throws Exception {
			return Tuple3.of(-1L, -1L, Double.MAX_VALUE);
		}
	}

	public static final class MessengerSendFunction
		extends ScatterFunction <Long, Tuple3 <Long, Long, Double>, Tuple3 <Long, Long, Double>, Double> {
		private static final long serialVersionUID = -2891289370485322356L;

		@Override
		public void sendMessages(Vertex <Long, Tuple3 <Long, Long, Double>> vertex) {
			//向每个邻点发送信息，主要是label和分数。
			//send msg(label and score) along with edges, to each neighbor.
			if(vertex.getValue().f0 < 0 || vertex.getValue().f1 < 0) {
				return;
			}
			for (Edge <Long, Double> edge : getEdges()) {
				sendMessageTo(edge.getTarget(), new Tuple3 <>(vertex.getValue().f0, vertex.getId(),
					vertex.getValue().f2 + edge.getValue()));
			}
		}
	}

	public static final class VertexUpdater
		extends GatherFunction <Long, Tuple3 <Long, Long, Double>, Tuple3 <Long, Long, Double>> {

		private static final long serialVersionUID = 612347525612715614L;

		public VertexUpdater() {}

		@Override
		public void updateVertex(Vertex <Long, Tuple3 <Long, Long, Double>> vertex,
								 MessageIterator <Tuple3 <Long, Long, Double>> inMessages) {
			double minWeight = vertex.f1.f2;
			Tuple3 <Long, Long, Double> minTuple = new Tuple3 <>();
			for (Tuple3 <Long, Long, Double> message : inMessages) {
				if (message.f2 < minWeight) {
					minWeight = message.f2;
					minTuple = message;
				}
			}
			if (minWeight < vertex.f1.f2) {
				setNewVertexValue(minTuple);
			}
		}
	}

}