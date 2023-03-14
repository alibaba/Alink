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
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
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
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.CommunityDetectionClassifyBatchOp.CommunityDetection.ClassifyLabelMerger;
import com.alibaba.alink.operator.batch.graph.CommunityDetectionClassifyBatchOp.CommunityDetection.ClassifyMessageGroupFunction;
import com.alibaba.alink.params.graph.CommunityDetectionClassifyParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

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
@NameEn("Common Detection Classify")
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

	/**
	 * Community Detection Algorithm.
	 * <p>
	 * <p>The Vertex values of the input Graph provide the initial label assignments.
	 * <p>
	 * <p>Initially, each vertex is assigned a tuple formed of its own initial value along with a score equal to 1.0.
	 * The vertices propagate their labels and max scores in iterations, each time adopting the label with the
	 * highest score from the list of received messages. The chosen label is afterwards re-scored using the fraction
	 * delta/the superstep number. Delta is passed as a parameter and has 0.5 as a default value.
	 */
	public static class CommunityDetection {

		private int maxIterations;
		private double delta; /*(0,1) default 0.5 */
		private int k; /* 1/k nodes not update info */

		/**
		 * Creates a new Community Detection algorithm instance.
		 * The algorithm converges when vertices no longer update their value
		 * or when the maximum number of iterations is reached.
		 *
		 * @param maxIterations The maximum number of iterations to run.
		 * @param delta         The hop attenuation parameter. Its default value is 0.5.
		 * @see <a href="http://arxiv.org/pdf/0808.2633.pdf">
		 * Towards real-time community detection in large networks</a>
		 */
		public CommunityDetection(int maxIterations, double delta, int k) {
			this.maxIterations = maxIterations;
			this.delta = delta;
			this.k = k;
		}

		public Graph <Long, Double, Double> runCluster(Graph <Long, Double, Double> graph) {
			return operation(graph);

		}

		public Graph <Long, Double, Double> runCluster(DataSet <Edge <Long, Double>> edges, Boolean directed) {

			Graph <Long, Double, Double> graph;
			if (directed) {
				graph = Graph.fromDataSet(edges, getExecutionEnvironmentFromDataSets(edges))
					.mapVertices(new MapVertices()).getUndirected();
			} else {
				graph = Graph.fromDataSet(edges, getExecutionEnvironmentFromDataSets(edges))
					.mapVertices(new MapVertices());
			}
			return operation(graph);

		}

		public Graph <Long, Double, Double> runClassify(Graph <Long, Tuple3 <Double, Double, Integer>, Double> graph) {
			//this operation is the last operation of the method operation.
			Graph <Long, Double, Double> res = graph.runScatterGatherIteration(new LabelMessengerClassify(),
				new VertexLabelUpdater(delta, k, false), maxIterations)
				.mapVertices(new RemoveScoreFromVertexValuesMapper());
			return res;
		}

		public static Graph <Long, Double, Double> operation(Graph <Long, Double, Double> graph, int maxIterations,
															 double delta, int k) {
			//if clusterAlgorithm is true, it means to run cluster algorithm.
			//vertex的三个变量分别为：点的id，点的label和点的分数。
			// tuple3: label, score and k.
			DataSet <Vertex <Long, Tuple3 <Double, Double, Integer>>> initializedVertices = graph
				.getVertices()
				.map(new AddScoreToVertexValuesMapper(k));

			//构造新的图
			Graph <Long, Tuple3 <Double, Double, Integer>, Double> graphWithScoredVertices =
				Graph.fromDataSet(initializedVertices, graph.getEdges(), graph.getContext());
			//主要的scatter-gather函数

			Graph <Long, Double, Double> res = graphWithScoredVertices
				.runScatterGatherIteration(new LabelMessengerCluster(),
				new VertexLabelUpdater(delta, k, true), maxIterations)
				.mapVertices(new RemoveScoreFromVertexValuesMapper());
			return res;

		}

		private Graph <Long, Double, Double> operation(Graph <Long, Double, Double> graph) {
			return operation(graph, maxIterations, delta, k);

		}

		public static final class LabelMessengerCluster
			extends ScatterFunction <Long, Tuple3 <Double, Double, Integer>, Tuple2 <Double, Double>, Double> {

			private static final long serialVersionUID = 8226284268536491551L;

			@Override
			public void sendMessages(Vertex <Long, Tuple3 <Double, Double, Integer>> vertex) {
				//向每个邻点发送信息，主要是label和分数。
				//send msg(label and score) along with edges, to each neighbor.

				for (Edge <Long, Double> edge : getEdges()) {
					sendMessageTo(edge.getTarget(), new Tuple2 <>(vertex.getValue().f0,
						vertex.getValue().f1 * edge.getValue()));//将label和分数乘label传递。
				}
			}
		}

		public static final class LabelMessengerClassify
			extends ScatterFunction <Long, Tuple3 <Double, Double, Integer>, Tuple2 <Double, Double>, Double> {

			private static final long serialVersionUID = 542036977788012860L;

			@Override
			public void sendMessages(Vertex <Long, Tuple3 <Double, Double, Integer>> vertex) {
				//向每个邻点发送信息，主要是label和分数。
				//send msg(label and score) along with edges, to each neighbor.
				//in community detection cluster algorithm, set the unlabelled vertices as -1.
				//if the label is -1, it won't send msg to its neighbor.
				if (Math.abs(vertex.getValue().f0 + 1) > 1e-4) {
					for (Edge <Long, Double> edge : getEdges()) {
						sendMessageTo(edge.getTarget(), new Tuple2 <>(vertex.getValue().f0,
							vertex.getValue().f1 * edge.getValue()));//将label和分数乘label传递。
					}
				}
			}
		}

		public static final class VertexLabelUpdater
			extends GatherFunction <Long, Tuple3 <Double, Double, Integer>, Tuple2 <Double, Double>> {

			private static final long serialVersionUID = -4158941524263734994L;
			private double delta;
			private int k;
			private Boolean clusterAlgorithm;

			public VertexLabelUpdater(double delta, int k, Boolean clusterAlgorithm) {
				this.delta = delta;
				this.k = k;
				this.clusterAlgorithm = clusterAlgorithm;
			}

			@Override
			public void updateVertex(Vertex <Long, Tuple3 <Double, Double, Integer>> vertex,
									 MessageIterator <Tuple2 <Double, Double>> inMessages) {
				//as for the cluster algorithm, each point has a sign and each iteration only update one label.
				//as for the classify algorithm, update only when the vertex label equals to -1.
				if (clusterAlgorithm && vertex.f1.f2 % k == 0) {
					vertex.f1.f2 = 1;
					setNewVertexValue(vertex.f1);
				} else if (!((!clusterAlgorithm) && Math.abs(vertex.getValue().f0 + 1) > 1e-4)) {
					//the following refers to flink code.
					//针对当前的这个点，它收到的label以及score的集合
					// we would like these two maps to be ordered
					Map <Double, Double> receivedLabelsWithScores = new TreeMap <>();
					Map <Double, Double> labelsWithHighestScore = new TreeMap <>();

					for (Tuple2 <Double, Double> message : inMessages) {
						// split the message into received label and score
						double receivedLabel = message.f0;
						double receivedScore = message.f1;
						//如果之前没接收到这个label，就将这个label存入TreeMap中；如果接收过，则将score值累加。
						// if the label was received before
						if (receivedLabelsWithScores.containsKey(receivedLabel)) {
							double newScore = receivedScore + receivedLabelsWithScores.get(receivedLabel);
							receivedLabelsWithScores.put(receivedLabel, newScore);
						} else {
							// first time we see the label
							receivedLabelsWithScores.put(receivedLabel, receivedScore);
						}
						//将每个点最大的score存入。
						// store the labels with the highest scores
						if (labelsWithHighestScore.containsKey(receivedLabel)) {
							double currentScore = labelsWithHighestScore.get(receivedLabel);
							if (currentScore < receivedScore) {
								// record the highest score
								labelsWithHighestScore.put(receivedLabel, receivedScore);
							}
						} else {
							// first time we see this label
							labelsWithHighestScore.put(receivedLabel, receivedScore);
						}
					}
					if (receivedLabelsWithScores.size() > 0) {
						//如果等于0则说明没有收到信息。那这样也不可能执行迭代。
						// find the label with the highest score from the ones received
						double maxScore = -Double.MAX_VALUE;
						//找到当前最大的累加score以及label。并没考虑label重复的情况。
						double maxScoreLabel = vertex.getValue().f0;
						for (Double curLabel : receivedLabelsWithScores.keySet()) {
							if (receivedLabelsWithScores.get(curLabel) > maxScore) {
								maxScore = receivedLabelsWithScores.get(curLabel);
								maxScoreLabel = curLabel;
							}
						}
						//如果累加最大score的label不是现在点的label，则对score进行更新。
						// find the highest score of maxScoreLabel
						double highestScore = labelsWithHighestScore.get(maxScoreLabel);
						// re-score the new label
						if (maxScoreLabel != vertex.getValue().f0) {
							highestScore -= delta / getSuperstepNumber();
						}
						// else delta = 0
						// update own label
						vertex.f1.f2 += 1;
						setNewVertexValue(new Tuple3 <>(maxScoreLabel, highestScore, vertex.f1.f2));
					//} else {
					//	setNewVertexValue(vertex.f1);
					}
				}
			}
		}

		@ForwardedFields("f0")
		//initialize vertex set and add score.
		public static class AddScoreToVertexValuesMapper
			implements MapFunction <Vertex <Long, Double>, Vertex <Long, Tuple3 <Double, Double, Integer>>> {
			private static final long serialVersionUID = 8094922891125063932L;
			private Random seed;
			private int k;


			public AddScoreToVertexValuesMapper(Integer k) {
				this.seed = new Random();
				this.k = k;
			}

			public Vertex <Long, Tuple3 <Double, Double, Integer>> map(Vertex <Long, Double> vertex) {
				return new Vertex <>(vertex.getId(), new Tuple3 <>(vertex.getValue(), 1.0, (int) (vertex.getId() % k))); //todo 改
			}
		}

		public static class RemoveScoreFromVertexValuesMapper
			implements MapFunction <Vertex <Long, Tuple3 <Double, Double, Integer>>, Double> {

			private static final long serialVersionUID = 1275388361160360632L;

			@Override
			public Double map(Vertex <Long, Tuple3 <Double, Double, Integer>> vertex) throws Exception {
				return vertex.getValue().f0;
			}
		}

		public static class MapVertices implements MapFunction <Vertex <Long, NullValue>, Double> {

			private static final long serialVersionUID = 456358729085831893L;

			@Override
			public Double map(Vertex <Long, NullValue> value) throws Exception {
				return value.f0.doubleValue();
			}
		}

		public static class ClusterMessageGroupFunction
			implements GroupReduceFunction <Tuple3 <Long, Long, Float>, Tuple3 <Long, Long, Integer>> {

			@Override
			public void reduce(Iterable <Tuple3 <Long, Long, Float>> values,
							   Collector <Tuple3 <Long, Long, Integer>> out) throws Exception {
				Map <Long, Float> receivedLabelsWithScores = new HashMap <>();
				Long lastNodeId = 0L;
				for (Tuple3 <Long, Long, Float> message : values) {
					lastNodeId = message.f0;
					Long receivedLabel = message.f1;
					float receivedScore = message.f2;
					receivedLabelsWithScores.put(receivedLabel,
						receivedScore + receivedLabelsWithScores.getOrDefault(receivedLabel, 0.0F));
				}
				float maxScore = ((Integer) Integer.MIN_VALUE).floatValue();
				Long maxScoreLabel = Long.MIN_VALUE;
				for (Long curLabel : receivedLabelsWithScores.keySet()) {
					float weight = receivedLabelsWithScores.get(curLabel);
					if (weight > maxScore) {
						maxScore = receivedLabelsWithScores.get(curLabel);
						maxScoreLabel = curLabel;
					} else if (Math.abs(weight - maxScore) <= 1e-4 && curLabel < maxScoreLabel) {
						maxScoreLabel = curLabel;
					}
				}
				out.collect(Tuple3.of(lastNodeId, maxScoreLabel, receivedLabelsWithScores.size()));
			}
		}

		public static class ClusterLabelMerger
			implements JoinFunction <Tuple3 <Long, Long, Integer>, Tuple3 <Long, Long, Float>,
					Tuple4 <Long, Long, Float, Boolean>> {

			@Override
			public Tuple4 <Long, Long, Float, Boolean> join(Tuple3 <Long, Long, Integer> first,
															Tuple3 <Long, Long, Float> second)
				throws Exception {
				if (null == first || second.f1.equals(first.f1)) {
					return Tuple4.of(second.f0, second.f1, second.f2, false);
				} else {
					if (Math.random() > 1.0 / (first.f2 + 1)) {
						return Tuple4.of(second.f0, first.f1, second.f2, true);
					} else {
						return Tuple4.of(second.f0, second.f1, second.f2, true);
					}
				}
			}
		}

		protected static class ClassifyMessageGroupFunction
			implements GroupReduceFunction <Tuple3 <Long, Integer, Float>, Tuple3 <Long, Integer, Float>> {

			@Override
			public void reduce(Iterable <Tuple3 <Long, Integer, Float>> values,
							   Collector <Tuple3 <Long, Integer, Float>> out) throws Exception {
				Map <Integer, Float> labelScoresMap = new HashMap <>();
				Long currentNode = 0L;
				float totalWeight = 0.F;
				for (Tuple3 <Long, Integer, Float> value : values) {
					if (value.f2 < 0) {
						continue;
					}
					currentNode = value.f0;
					labelScoresMap.put(value.f1, labelScoresMap.getOrDefault(value.f1, 0.F) + value.f2);
					totalWeight += value.f2;
				}
				Integer maxLabel = -1;
				float maxScore = 0.F;
				for (Entry <Integer, Float> entry : labelScoresMap.entrySet()) {
					if (entry.getValue() > maxScore) {
						maxLabel = entry.getKey();
						maxScore = entry.getValue();
					}
				}
				if (maxLabel < 0) {
					out.collect(Tuple3.of(currentNode, -1, 0F));
				} else {
					out.collect(Tuple3.of(currentNode, maxLabel, maxScore / totalWeight));
				}
			}
		}

		protected static class ClassifyLabelMerger
			implements JoinFunction <Tuple3 <Long, Integer, Float>, Tuple4 <Long, Integer, Float, Boolean>,
			Tuple5 <Long, Integer, Float, Boolean, Boolean>> {

			private float delta;

			ClassifyLabelMerger(double delta) {
				this.delta = ((Number) delta).floatValue();
			}

			@Override
			public Tuple5 <Long, Integer, Float, Boolean, Boolean> join(Tuple3 <Long, Integer, Float> first,
																		Tuple4 <Long, Integer, Float, Boolean> second)
				throws Exception {
				Tuple5 res = null;
				// if node not exists in generated label dataset,
				// or node exists in input classify nodes (second.f3 = true)
				// or new label with negative weight
				// or node already been set label in preprocess iteration
				// node will keep label unchanged.
				if (null == first || second.f3 || first.f2 < 0 || second.f1 >= 0) {
					res = Tuple5.of(second.f0, second.f1, second.f2, second.f3, false);
				} else {
					if (second.f2 == first.f2) {
						res = Tuple5.of(second.f0, second.f1, second.f2 * delta + first.f2 * (1 - delta), second.f3, false);
					} else {
						res = Tuple5.of(second.f0, first.f1, first.f2, second.f3, true);
					}
				}
				return res;
			}
		}
	}
}
