package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.nlp.walk.Node2VecWalkParams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * This algorithm realizes the node2vec Walk.
 * The graph is saved in the form of DataSet of edges,
 * and the output random walk consists of vertices in order.
 * In the DataSet iteration loop, the neighbors of vertex t needs to
 * be sent to v to judge the weight of edges.
 * In this algorithm, we record vertices in the form of edges.
 * The source represents the id of vertex, and the target represents who it denotes to
 * and the value represents the weight.
 * <p>
 * If a random walk terminals before reach the walk length, it won't continue and
 * we only need to return this short walk.
 */

public final class Node2VecWalkBatchOp extends BatchOperator <Node2VecWalkBatchOp>
	implements Node2VecWalkParams <Node2VecWalkBatchOp> {

	public static final String PATH_COL_NAME = "path";
	private static final long serialVersionUID = 5772364018494433734L;

	public Node2VecWalkBatchOp() {
		super(new Params());
	}

	public Node2VecWalkBatchOp(Params params) {
		super(params);
	}

	@Override
	public Node2VecWalkBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		double pReciVal = 1 / getParams().get(Node2VecWalkParams.P);
		double qReciVal = 1 / getParams().get(Node2VecWalkParams.Q);
		Integer walkNum = getWalkNum();
		Integer walkLength = getWalkLength();
		String delimiter = getDelimiter();
		String node0ColName = getSourceCol();
		String node1ColName = getTargetCol();
		String valueColName = getWeightCol();
		Boolean isToUnDigraph = getIsToUndigraph();

		int node0Idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), node0ColName);
		int node1Idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), node1ColName);
		int valueIdx = (valueColName == null) ? -1 : TableUtil.findColIndexWithAssertAndHint(in.getColNames(),
			valueColName);

		// merge same edge and sum the weight value
		DataSet <Edge <String, Double>>
			edge = WalkUtils.buildEdge(in.getDataSet(), node0Idx, node1Idx, valueIdx, isToUnDigraph);
		// building node mapping.
		DataSet <Tuple2 <String, Integer>> nodeMapping = WalkUtils.getStringIndexMapping(edge);
		DataSet <Edge <Integer, Double>> intEdge = WalkUtils.string2Index(edge, nodeMapping);

		//initialize the length of arrays to walkLength, and repeat it walkNum times,
		//and then write the start point into the arrays.
		DataSet <Integer[]> initialWalk = intEdge
			.distinct(0)
			.mapPartition(new InitialMap(walkNum, walkLength)).name("build_init_n2v_walk");

		DataSet <Tuple3 <Integer, Integer[], Double[]>>
			neighbours = intEdge.groupBy(0).reduceGroup(
			new GroupReduceFunction <Edge <Integer, Double>, Tuple3 <Integer, Integer[], Double[]>>() {
				private static final long serialVersionUID = -7437289818469555926L;

				@Override
				public void reduce(Iterable <Edge <Integer, Double>> values,
								   Collector <Tuple3 <Integer, Integer[], Double[]>> out)
					throws Exception {
					Integer left = null;
					List <Tuple2 <Integer, Double>> localNeighbours = new ArrayList <>();
					boolean first = true;
					for (Edge <Integer, Double> e : values) {
						if (first) {
							left = e.f0;
							first = false;
						}
						localNeighbours.add(Tuple2.of(e.f1, e.f2));
					}
					Tuple2 <Integer, Double>[] ret = new Tuple2[localNeighbours.size()];
					for (int i = 0; i < ret.length; ++i) {
						ret[i] = localNeighbours.get(i);
					}

					Arrays.sort(ret, new Comparator <Tuple2 <Integer, Double>>() {
						@Override
						public int compare(Tuple2 <Integer, Double> o1, Tuple2 <Integer, Double> o2) {
							return o1.f0.compareTo(o2.f0);
						}
					});
					Integer[] neis = new Integer[localNeighbours.size()];
					Double[] weights = new Double[localNeighbours.size()];
					for (int i = 0; i < ret.length; ++i) {
						neis[i] = ret[i].f0;
						weights[i] = ret[i].f1;
					}
					out.collect(Tuple3.of(left, neis, weights));
				}
			}).name("build_n2v_neighbors");
		//During the walk, we need to get the neighbor information of each vertex.
		//Fortunately initial walk covers all the vertices as start, so we write the
		//neighbor information in the initial state.
		//each iteration update the dic and link it with the up-to-date end points.
		DataSet <Tuple2 <Integer[], Integer[]>> iniState = initialWalk.coGroup(neighbours)
			.where(new SelectIniWalk())
			.equalTo(new SelectKeyLatter())
			.with(new AddNeighbor()).name("build_init_state_with_neighbors");

		//iteration starts. Because the start step has run, the iteration time becomes walkLength-1
		IterativeDataSet <Tuple2 <Integer[], Integer[]>> state = iniState
			.iterate(walkLength - 1);
		//main iteration step
		DataSet <Tuple2 <Integer[], Integer[]>> outState = state.closeWith(
			state.coGroup(neighbours)
				.where(new SelectKeyFormer(walkLength))
				.equalTo(new SelectKeyLatter())
				.with(new Sampling(pReciVal, qReciVal)).name("n2v_cogroup_iteration"));
		DataSet <Integer[]> intOut = outState.map(new MapOutput());

		DataSet <String[]> out = WalkUtils.index2String(intOut, nodeMapping, walkLength);

		this.setOutputTable(WalkUtils.transString(out, new String[] {PATH_COL_NAME}, delimiter, getMLEnvironmentId()));
		return this;
	}

	public static class InitialMap implements MapPartitionFunction <Edge <Integer, Double>, Integer[]> {
		private static final long serialVersionUID = -5165723205282442477L;
		private Integer walkNum;
		private Integer walkLength;

		private InitialMap(Integer walkNum, Integer walkLength) {
			this.walkNum = walkNum;
			this.walkLength = walkLength;
		}

		@Override
		public void mapPartition(Iterable <Edge <Integer, Double>> values, Collector <Integer[]> out) throws
			Exception {
			for (Edge <Integer, Double> edge : values) {
				for (int i = 0; i < this.walkNum; i++) {
					Integer[] temp = new Integer[this.walkLength];
					for (int j = 0; j < this.walkLength; j++) {
						temp[j] = null;
					}
					temp[0] = edge.f0;
					out.collect(temp);
				}
			}
		}
	}

	public static class SelectIniWalk implements KeySelector <Integer[], Integer> {
		private static final long serialVersionUID = -8135312842049371979L;

		@Override
		public Integer getKey(Integer[] value) throws Exception {
			return value[0];
		}
	}

	public static class AddNeighbor implements CoGroupFunction <Integer[],
		Tuple3 <Integer, Integer[], Double[]>,
		Tuple2 <Integer[], Integer[]>> {
		private static final long serialVersionUID = -6769474188052613936L;

		@Override
		public void coGroup(Iterable <Integer[]> first,
							Iterable <Tuple3 <Integer, Integer[], Double[]>> second,
							Collector <Tuple2 <Integer[], Integer[]>> out) {
			Integer[] dic = null;
			for (Tuple3 <Integer, Integer[], Double[]> edge : second) {
				Integer[] val = edge.f1;
				dic = new Integer[val.length];
				for (int i = 0; i < val.length; ++i) {
					dic[i] = val[i];
				}
			}

			for (Integer[] i : first) {
				out.collect(new Tuple2 <>(i, dic));
			}
		}
	}

	/**
	 * groupBy with the last element whose value is not -1.
	 */
	public static class SelectKeyFormer implements KeySelector <Tuple2 <Integer[], Integer[]>, Integer> {
		private static final long serialVersionUID = -6016529090300300706L;
		private Integer walkLength;

		public SelectKeyFormer(Integer walkLength) {
			this.walkLength = walkLength;
		}

		@Override
		public Integer getKey(Tuple2 <Integer[], Integer[]> value) throws Exception {
			Integer[] walkValue = value.f0;
			for (int i = 0; i < this.walkLength - 1; i++) {
				if (walkValue[i] != null && walkValue[i + 1] == null) {
					return walkValue[i];
				}
			}
			return null;
		}
	}

	protected static class SelectKeyLatter
		implements KeySelector <Tuple3 <Integer, Integer[], Double[]>, Integer> {
		private static final long serialVersionUID = 1108707031960431089L;

		@Override
		public Integer getKey(Tuple3 <Integer, Integer[], Double[]> value) throws Exception {
			return value.f0;
		}
	}

	public static class Sampling extends RichCoGroupFunction <Tuple2 <Integer[], Integer[]>,
		Tuple3 <Integer, Integer[], Double[]>,
		Tuple2 <Integer[], Integer[]>> {
		private static final long serialVersionUID = -4323568029368465680L;
		private RandSrc randSrc;

		public Sampling(Double p, Double q) {
			this.randSrc = new RandSrc(p, q);
		}

		@Override
		public void coGroup(Iterable <Tuple2 <Integer[], Integer[]>> first,
							Iterable <Tuple3 <Integer, Integer[], Double[]>> second,
							Collector <Tuple2 <Integer[], Integer[]>> out) {

			int step = getIterationRuntimeContext().getSuperstepNumber();
			//get all the neighbors.
			Tuple2 <Integer[], Double[]> dic = null;
			for (Tuple3 <Integer, Integer[], Double[]> t2 : second) {
				dic = Tuple2.of(t2.f1, t2.f2);
			}
			//if the vertices has no out degree, the weight is -1, and then exit at once.
			if (dic == null) {
				for (Tuple2 <Integer[], Integer[]> i : first) {
					out.collect(i);
				}
			} else {
				//weighted sampling with "second"
				for (Tuple2 <Integer[], Integer[]> t2 : first) {
					Integer[] path = t2.f0;
					Integer[] neighbor = t2.f1;
					boolean flag = false;

					//if j==1, it mean it is the second iteration, so the input of neighbor is null, and
					//send the origin neighbor to the next iteration. Otherwise send the neighbor of second
					if (step == 1) {
						path[step] = randSrc.run(dic, null, path[step - 1]);
						flag = true;
					} else {
						path[step] = randSrc.run(dic, neighbor, path[step - 2]);
					}

					if (flag) {
						out.collect(Tuple2.of(path, t2.f1));
					} else {
						Integer[] ret = dic.f0;

						out.collect(Tuple2.of(path, ret));
					}
				}
			}
		}
	}

	/**
	 * execute weighted sampling, output the chosen target
	 */
	public static class RandSrc implements Serializable {
		private static final long serialVersionUID = -3974746808883287887L;
		private Double p;
		private Double q;
		private Random seed;

		public RandSrc(Double p, Double q) {
			this.p = p;
			this.q = q;
			//it's a serial operation. only when construct RandSrc, we well create a new Random.
			this.seed = new Random(2018);
		}

		public Integer run(Tuple2 <Integer[], Double[]> dicOrigin, Integer[] neighbor, Integer id) {
			// update the weight of dic due to neighbor relationship
			Tuple2 <Integer[], Double[]> dic = Tuple2.of(dicOrigin.f0, dicOrigin.f1.clone());

			//if neighbor is null, we needn't to update dic.
			if (neighbor != null) {
				Set <Integer> set = new HashSet <>(neighbor.length);
				for (int i = 0; i < neighbor.length; ++i) {
					set.add(neighbor[i]);
				}
				//the same id mean distance is 0; if the id is a neighbor, it mean 1; otherwise 2
				for (int i = 0; i < dic.f0.length; i++) {
					if (!(set.contains(dic.f0[i]))) {
						dic.f1[i] *= this.q;
					} else if (dic.f0[i].equals(id)) {
						dic.f1[i] *= this.p;
					}
				}
			}
			if (dic.f0.length > 1) {
				for (int i = 1; i < dic.f0.length; i++) {
					dic.f1[i] += dic.f1[i - 1];
				}
				Double s = dic.f1[dic.f1.length - 1];
				for (int i = 0; i < dic.f1.length; i++) {
					dic.f1[i] /= s;
				}
			}

			int idx = Arrays.binarySearch(dic.f1, 0, dic.f1.length - 1, seed.nextDouble());

			return dic.f0[Math.abs(idx) - 1];
		}
	}

	/**
	 * as for directed graph, remove all the -1 and then get the path
	 **/
	public static class MapOutput implements MapFunction <Tuple2 <Integer[], Integer[]>, Integer[]> {
		private static final long serialVersionUID = -4996358404446902515L;

		@Override
		public Integer[] map(Tuple2 <Integer[], Integer[]> value) throws Exception {
			List <Integer> temp = new ArrayList <>(value.f1.length);
			for (Integer i : value.f0) {
				if (i != null) {
					temp.add(i);
				} else {
					break;
				}
			}
			//transform list to array, it may cause loss in memory and speed
			Integer[] ret = new Integer[temp.size()];
			temp.toArray(ret);
			return ret;
		}
	}
}
