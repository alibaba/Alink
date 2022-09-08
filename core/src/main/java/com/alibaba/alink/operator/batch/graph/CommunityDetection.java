package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.BatchOperator;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

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
public class CommunityDetection {

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
			graph = Graph.fromDataSet(edges, BatchOperator.getExecutionEnvironmentFromDataSets(edges))
				.mapVertices(new MapVertices()).getUndirected();
		} else {
			graph = Graph.fromDataSet(edges, BatchOperator.getExecutionEnvironmentFromDataSets(edges))
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

