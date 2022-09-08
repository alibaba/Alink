package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexInDegree;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;
import org.apache.flink.types.LongValue;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * As for a legal input forest, there is not any nodes have more than one precursor, or there is no ring.
 * If the input is illegal, exception will be raised.
 * Considering the situation that a no-zero-degree-root ring exists, we set the max iteration being 50.
 * There cannot exist a input has a depth larger than 2^50. After 50 iterations, if there exists nodes with
 * negative depth value, exception will be raised.
 * <p>
 * The time complexity of this algorithm is T(log n), relating to the depth.
 * Algorithm description: each vertex has a Tuple3 value, the three elements represent its id, its father's id,
 * as well as the relative depth between it and its father. If the relative depth is positive, it mean this vertex
 * has got linked to a root node, otherwise the negative depth denotes the difference between it and its father.
 * The vertices with depth not less than zero have get linked to a root, they won't send back messages unless
 * they receive messages from their sons. As for the son vertices, they send messages to their father nodes,
 * and update father nodes according to the back replies, until they get linked to a root.
 * <p>
 * <p>
 * In a legal tree, each node have at most one pre-node. So if one node has more than one pre-node, the algorithm
 * will judge it illegal.
 *
 * @author qingzhao
 */

public class TreeDepth {
	public Integer maxIter;

	/**
	 * @param maxIter The maximum number of iterations to run.
	 */
	public TreeDepth(int maxIter) {
		this.maxIter = maxIter;
	}

	private Graph <Long, Tuple3 <Long, Long, Double>, Double> operation(Graph <Long, Double, Double> graph) {
		DataSet <Vertex <Long, LongValue>> inVertexTemp;
		//choose the root nodes, whose value is 0, while others' are 1.
		try {
			inVertexTemp = graph.run(new VertexInDegree <Long, Double, Double>()
				.setIncludeZeroDegreeVertices(true));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		DataSet <Vertex <Long, Tuple3 <Long, Long, Double>>> graphNewVertices = inVertexTemp.map(
			new MapVertexValue());
		DataSet <Edge <Long, Double>> edgeDataSet = graph.getEdges().map(
			new ReverseEdge());
		Graph <Long, Tuple3 <Long, Long, Double>, Double> graphExecute = Graph.fromDataSet(
			graphNewVertices,
			edgeDataSet,
			BatchOperator.getExecutionEnvironmentFromDataSets(graphNewVertices, edgeDataSet));
		VertexCentricConfiguration parameters = new VertexCentricConfiguration();
		parameters.setName("tree depth iteration");
		Graph <Long, Tuple3 <Long, Long, Double>, Double> res = graphExecute
			.runVertexCentricIteration(
				new Execute(),
				null,
				maxIter, parameters);
		return res;
	}

	public DataSet <Tuple3 <Long, Long, Double>> run(Graph <Long, Double, Double> graph) {
		return operation(graph).getVertices().map(
			new JudgeTupleIllegal());
	}

	//initial step, set the initial vertex value
	public static class MapVertexValue
		implements MapFunction <Vertex <Long, LongValue>, Vertex <Long, Tuple3 <Long, Long, Double>>> {
		private static final long serialVersionUID = 2154022863365357679L;

		@Override
		public Vertex <Long, Tuple3 <Long, Long, Double>> map(Vertex <Long, LongValue> value) {
			return value.f1.getValue() == 0 ?
				new Vertex <>(value.f0, new Tuple3 <>(value.f0, value.f0, 0.)) :
				new Vertex <>(value.f0, new Tuple3 <>(value.f0, value.f0, -1.));
		}
	}

	public static class ReverseEdge implements MapFunction <Edge <Long, Double>, Edge <Long, Double>> {
		private static final long serialVersionUID = 7575794558756147475L;

		@Override
		public Edge <Long, Double> map(Edge <Long, Double> value) {
			if (value.f2 <= 0) {
				throw new RuntimeException("Edge " + value + " is illegal. Edge weight must be positive!");
			}
			return new Edge <>(value.f1, value.f0, value.f2);
		}
	}

	public static class Execute extends
		ComputeFunction <Long, Tuple3 <Long, Long, Double>, Double, Tuple3 <Long, Long, Double>> {
		private static final long serialVersionUID = -2503583975560433984L;

		@Override
		public void compute(Vertex <Long, Tuple3 <Long, Long, Double>> vertex,
							MessageIterator<Tuple3 <Long, Long, Double>> messages)
			throws Exception {
			//initial step. Vertices besides roots send message to their fathers and change their values.
			if (vertex.f1.f1.equals(vertex.f0) && vertex.f1.f1.equals(vertex.f1.f0) && vertex.f1.f2 != 0) {
				//if a vertex send message to more than one vertex, it mean that the vertex has more than one
				// precursor.
				boolean flag = false;
				for (Edge <Long, Double> edge : getEdges()) {
					if (!flag) {
						Tuple3 <Long, Long, Double> temp = new Tuple3 <>(vertex.f1.f0, edge.f1, -edge.f2);
						sendMessageTo(edge.getTarget(), temp);
						setNewVertexValue(temp);
						flag = true;
					} else {
						throw new Exception("illegal input!!!");
					}
				}
			} else {
				//when receiving a message, judge it is from father or son.
				for (Tuple3 <Long, Long, Double> msg : messages) {
					//receive message from son
					if (msg.f1.equals(vertex.f0)) {
						//send message to its son
						sendMessageTo(msg.f0, vertex.f1);
					} else {
						//receive message from father
						if (msg.f2 >= 0) {
							setNewVertexValue(new Tuple3 <>(vertex.f0, msg.f1, msg.f2 - vertex.f1.f2));
						} else {
							Tuple3 <Long, Long, Double> temp = new Tuple3 <>(vertex.f0, msg.f1, msg.f2 + vertex.f1.f2);
							setNewVertexValue(temp);
							sendMessageTo(temp.f1, temp);
						}
					}
				}
			}
		}
	}

	public static class JudgeTupleIllegal
		implements MapFunction <Vertex <Long, Tuple3 <Long, Long, Double>>, Tuple3 <Long, Long, Double>> {
		private static final long serialVersionUID = 858956933724773542L;

		@Override
		public Tuple3 <Long, Long, Double> map(Vertex <Long, Tuple3 <Long, Long, Double>> value) throws Exception {
			if (value.f1.f2 < 0) {
				throw new RuntimeException("illegal input!!!");
			}
			return value.f1;
		}
	}
}