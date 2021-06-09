package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.nlp.walk.RandomWalkParams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * This algorithm realizes the Random Walk. The graph is saved in the form of DataSet of edges, and the output random
 * walk consists of vertices in order. The iteration is in the form of DataSet so that it almost consumes no
 * communication memory.
 * <p>
 * If a random walk terminals before reach the walk length, it won't continue and we only need to return this short
 * walk.
 */
public final class RandomWalkBatchOp extends BatchOperator <RandomWalkBatchOp>
	implements RandomWalkParams <RandomWalkBatchOp> {

	public static final String PATH_COL_NAME = "path";
	private static final long serialVersionUID = 3726910334434343013L;

	public RandomWalkBatchOp(Params params) {
		super(params);
	}

	public RandomWalkBatchOp() {
		super(new Params());
	}

	@Override
	public RandomWalkBatchOp linkFrom(List <BatchOperator <?>> ins) {
		assert (ins.size() == 1);
		return linkFrom(ins.get(0));
	}

	@Override
	public RandomWalkBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		Integer walkNum = getWalkNum();
		final Integer walkLength = getWalkLength();
		String delimiter = getDelimiter();
		String node0ColName = getSourceCol();
		String node1ColName = getTargetCol();
		String valueColName = getWeightCol();
		Boolean isToUnDigraph = getIsToUndigraph();
		Boolean isWeightedSampling = getIsWeightedSampling();
		int node0Idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), node0ColName);
		int node1Idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), node1ColName);
		int valueIdx = (valueColName == null) ? -1 : TableUtil.findColIndexWithAssertAndHint(in.getColNames(),
			valueColName);

		// merge same edge and sum the weight value
		DataSet <Edge <String, Double>>
			edge = WalkUtils.buildEdge(in.getDataSet(), node0Idx, node1Idx, valueIdx, isToUnDigraph);
		// building node mapping.
		DataSet <Tuple2 <String, Integer>> nodeMapping = WalkUtils.getStringIndexMapping(edge);
		DataSet <Edge <Integer, Double>> initEdge = WalkUtils.string2Index(edge, nodeMapping);

		DataSet <Integer[]> pathInt;
		if (isWeightedSampling) {
			pathInt = weightedSampling(initEdge, nodeMapping, walkNum, walkLength);
		} else {
			pathInt = randomSampling(initEdge, nodeMapping, walkNum, walkLength);
		}
		DataSet <String[]> out = WalkUtils.index2String(pathInt, nodeMapping, walkLength);
		setOutputTable(WalkUtils.transString(out, new String[] {PATH_COL_NAME}, delimiter, getMLEnvironmentId()));
		return this;
	}

	private DataSet <Integer[]> randomSampling(DataSet <Edge <Integer, Double>> edge,
											   DataSet <Tuple2 <String, Integer>> nodeMapping,
											   final int walkNum,
											   final int walkLength) {
		long stateHandler = IterTaskObjKeeper.getNewHandle();
		DataSet <Integer> broadcastVar = nodeMapping.mapPartition(
			new RichMapPartitionFunction <Tuple2 <String, Integer>, Integer>() {
				private static final long serialVersionUID = -1124083745289502815L;

				@Override
				public void mapPartition(Iterable <Tuple2 <String, Integer>> values, Collector <Integer> out)
					throws Exception {
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						out.collect(1);
					}
				}
			});

		IterativeDataSet <Integer[]> state = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment()
			.fromElements(1).map(new MapFunction <Integer, Integer[]>() {
				private static final long serialVersionUID = 7041949788855206900L;

				@Override
				public Integer[] map(Integer value) throws Exception {
					return new Integer[1];
				}
			}).withBroadcastSet(broadcastVar, "broadVar").iterate(walkLength);

		// vertexId, neighbors
		DataSet <Tuple2 <Integer, Integer[]>>
			neighbours = edge.groupBy(0).reduceGroup(
			new GroupReduceFunction <Edge <Integer, Double>, Tuple2 <Integer, Integer[]>>() {
				private static final long serialVersionUID = -3815620281513545733L;

				@Override
				public void reduce(Iterable <Edge <Integer, Double>> values,
								   Collector <Tuple2 <Integer, Integer[]>> out)
					throws Exception {
					Integer left = null;
					List <Integer> neighbours = new ArrayList <>();
					boolean first = true;
					for (Edge <Integer, Double> e : values) {
						if (first) {
							left = e.f0;
							first = false;
						}
						neighbours.add(e.f1);
					}
					Integer[] ret = new Integer[neighbours.size()];
					for (int i = 0; i < ret.length; ++i) {
						ret[i] = neighbours.get(i);
					}
					out.collect(Tuple2.of(left, ret));
				}
			}).name("build_neighbor");

		// vertexId, walkdId, endPoint
		DataSet <Integer> startVertices = edge
			.distinct(0)
			.rebalance()
			.mapPartition(new MapPartitionFunction <Edge <Integer, Double>, Integer>() {
				@Override
				public void mapPartition(Iterable <Edge <Integer, Double>> values, Collector <Integer> out)
					throws Exception {
					for (Edge e : values) {
						out.collect((Integer) e.f0);
					}
				}
			});

		// localPath
		// taskId, vertexId, walkdId, endPoint
		DataSet <Tuple4 <Integer, Integer, Integer, Integer>> localPath = startVertices.mapPartition(
			new RichMapPartitionFunction <Integer, Tuple4 <Integer, Integer, Integer, Integer>>() {
				@Override
				public void mapPartition(Iterable <Integer> values,
										 Collector <Tuple4 <Integer, Integer, Integer, Integer>> out) throws Exception {
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					int superStepId = getIterationRuntimeContext().getSuperstepNumber();

					if (superStepId == 1) {
						// initialze the path in static memory and output the pathEnd
						// at step 1, the walkId is 0 by default (for efficiency), we need to expand it to numWalks
						// vertexId, walkId (zero by default), endPoint
						List <Integer> vids = new ArrayList <>();
						HashMap <Integer, Integer> nodeMap = new HashMap <>();
						int cnt = 0;
						for (Integer vid : values) {
							vids.add(vid);
							nodeMap.put(vid, cnt);
							cnt++;
							for (int walkId = 0; walkId < walkNum; walkId++) {
								out.collect(Tuple4.of(taskId, vid, walkId, vid));
							}
						}

						int numVertexInPartition = vids.size();
						RandomWalkStorage randomWalkStorage = new RandomWalkStorage(numVertexInPartition, walkNum,
							walkLength); // no initialization yet.
						randomWalkStorage.setMap(nodeMap);
						for (int idx = 0; idx < vids.size(); idx++) {
							int vertexId = vids.get(idx);
							for (int walkId = 0; walkId < walkNum; walkId++) {
								randomWalkStorage.updatePath(vertexId, walkId, 0, vertexId);
							}
						}
						IterTaskObjKeeper.put(stateHandler, taskId, randomWalkStorage);
					} else {
						// output the pathEnd
						RandomWalkStorage randomWalkStorage = IterTaskObjKeeper.get(stateHandler, taskId);
						int numVertexInPartition = randomWalkStorage.getNumVertex();
						int numWalksPerVertex = randomWalkStorage.getNumWalksPerVertex();
						for (int globalWalkId = 0; globalWalkId < numVertexInPartition * numWalksPerVertex;
							 globalWalkId++) {
							int startVertexId = randomWalkStorage.getVertexIdByPosition(globalWalkId, 0);
							int currentEndVertexId = randomWalkStorage.getVertexIdByPosition(globalWalkId,
								superStepId - 1);
							// taskId, vertexId, walkdId, endPoint
							out.collect(
								Tuple4.of(taskId, startVertexId, globalWalkId % numWalksPerVertex,
									currentEndVertexId));
						}
					}
				}
			}).withBroadcastSet(state, "state");

		// taskId, vertexId_walkId, endPointId
		DataSet <Tuple4 <Integer, Integer, Integer, Integer>> third = localPath.coGroup(neighbours)
			.where(new KeySelector <Tuple4 <Integer, Integer, Integer, Integer>, Integer>() {
				@Override
				public Integer getKey(Tuple4 <Integer, Integer, Integer, Integer> value) throws Exception {
					// taskId, vertexId, walkdId, endPointId
					return value.f3;
				}
			})
			.equalTo(new KeySelector <Tuple2 <Integer, Integer[]>, Integer>() {
				@Override
				public Integer getKey(Tuple2 <Integer, Integer[]> value) throws Exception {
					// vertexId, neighbors
					return value.f0;
				}
			})
			.with(new SamplingWithoutWeight()).name("iter_cogroup")
			.partitionCustom(new Partitioner <Integer>() {
				private static final long serialVersionUID = 4740147336323665645L;

				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0);

		DataSet <Integer[]> newState = third.mapPartition(
			new RichMapPartitionFunction <Tuple4 <Integer, Integer, Integer, Integer>, Integer[]>() {
				private static final long serialVersionUID = 4817837060612938072L;

				@Override
				public void mapPartition(Iterable <Tuple4 <Integer, Integer, Integer, Integer>> values,
										 Collector <Integer[]>
											 out)
					throws Exception {
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					int superStepId = getIterationRuntimeContext().getSuperstepNumber();
					RandomWalkStorage randomWalkStorage = IterTaskObjKeeper.get(stateHandler, taskId);

					if (superStepId < walkLength) {
						// update path
						for (Tuple4 <Integer, Integer, Integer, Integer> value : values) {
							// taskId, vertexId, walkId, endPointId
							int vertexId = value.f1;
							int walkId = value.f2;
							int endVertexId = value.f3;
							randomWalkStorage.updatePath(vertexId, walkId, superStepId, endVertexId);
						}
						out.collect(new Integer[1]);
					} else {
						// update the end point and emit the path
						int numVertex = randomWalkStorage.getNumVertex();
						int numWalksPerVertex = randomWalkStorage.getNumWalksPerVertex();
						for (int globalWalkId = 0; globalWalkId < numVertex * numWalksPerVertex; globalWalkId++) {
							out.collect(randomWalkStorage.getWalksById(globalWalkId));
						}
					}
				}
			});

		DataSet <Integer[]> out = state.closeWith(newState);
		IterTaskObjKeeper.clear(stateHandler);
		return out;
	}

	private DataSet <Integer[]> weightedSampling(DataSet <Edge <Integer, Double>> edge,
												 DataSet <Tuple2 <String, Integer>> nodeMapping,
												 int walkNum, int walkLength) {
		long stateHandler = IterTaskObjKeeper.getNewHandle();
		DataSet <Integer> broadcastVar = nodeMapping.mapPartition(
			new RichMapPartitionFunction <Tuple2 <String, Integer>, Integer>() {
				private static final long serialVersionUID = -5772354915094427783L;

				@Override
				public void mapPartition(Iterable <Tuple2 <String, Integer>> values, Collector <Integer> out)
					throws Exception {
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						out.collect(1);
					}
				}
			});

		IterativeDataSet <Integer[]> state = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment()
			.fromElements(1).map(new MapFunction <Integer, Integer[]>() {
				private static final long serialVersionUID = -7583109448839459212L;

				@Override
				public Integer[] map(Integer value) throws Exception {
					return new Integer[1];
				}
			}).withBroadcastSet(broadcastVar, "broadVar").iterate(walkLength);

		// build neighbors
		DataSet <Edge <Integer, Double>> edgesReduced = edge
			.groupBy(0).aggregate(Aggregations.SUM, 2);
		DataSet <Edge <Integer, Double>> edgesNormalized = edge
			.coGroup(edgesReduced)
			.where(0)
			.equalTo(0)
			.with(new EdgeNormalize()).name("edge_normalize");

		// vertexId, neighbors
		DataSet <Tuple2 <Integer, Tuple2 <Integer, Double>[]>>
			neighbours = edgesNormalized.groupBy(0).reduceGroup(
			new GroupReduceFunction <Edge <Integer, Double>, Tuple2 <Integer, Tuple2 <Integer, Double>[]>>() {
				private static final long serialVersionUID = 850971599894099253L;

				@Override
				public void reduce(Iterable <Edge <Integer, Double>> values,
								   Collector <Tuple2 <Integer, Tuple2 <Integer, Double>[]>> out)
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
							return o1.f1.compareTo(o2.f1);
						}
					});

					out.collect(Tuple2.of(left, ret));
				}
			}).name("build_neighbors");

		// vertexId, walkdId, endPoint
		DataSet <Integer> startVertices = edge
			.distinct(0)
			.rebalance()
			.mapPartition(new MapPartitionFunction <Edge <Integer, Double>, Integer>() {
				@Override
				public void mapPartition(Iterable <Edge <Integer, Double>> values, Collector <Integer> out)
					throws Exception {
					for (Edge e : values) {
						out.collect((Integer) e.f0);
					}
				}
			});

		// localPath
		// taskId, vertexId, walkdId, endPoint
		DataSet <Tuple4 <Integer, Integer, Integer, Integer>> localPath = startVertices.mapPartition(
			new RichMapPartitionFunction <Integer, Tuple4 <Integer, Integer, Integer, Integer>>() {
				@Override
				public void mapPartition(Iterable <Integer> values,
										 Collector <Tuple4 <Integer, Integer, Integer, Integer>> out) throws Exception {
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					int superStepId = getIterationRuntimeContext().getSuperstepNumber();

					if (superStepId == 1) {
						// initialze the path in static memory and output the pathEnd
						// at step 1, the walkId is 0 by default (for efficiency), we need to expand it to numWalks
						// vertexId, walkId (zero by default), endPoint
						List <Integer> vids = new ArrayList <>();
						HashMap <Integer, Integer> nodeMap = new HashMap <>();
						int cnt = 0;
						for (Integer vid : values) {
							vids.add(vid);
							nodeMap.put(vid, cnt);
							cnt++;
							for (int walkId = 0; walkId < walkNum; walkId++) {
								out.collect(Tuple4.of(taskId, vid, walkId, vid));
							}
						}

						int numVertexInPartition = vids.size();
						RandomWalkStorage randomWalkStorage = new RandomWalkStorage(numVertexInPartition, walkNum,
							walkLength); // no initialization yet.
						randomWalkStorage.setMap(nodeMap);
						for (int idx = 0; idx < vids.size(); idx++) {
							int vertexId = vids.get(idx);
							for (int walkId = 0; walkId < walkNum; walkId++) {
								randomWalkStorage.updatePath(vertexId, walkId, 0, vertexId);
							}
						}
						IterTaskObjKeeper.put(stateHandler, taskId, randomWalkStorage);
					} else {
						// output the pathEnd
						RandomWalkStorage randomWalkStorage = IterTaskObjKeeper.get(stateHandler, taskId);
						int numVertexInPartition = randomWalkStorage.getNumVertex();
						int numWalksPerVertex = randomWalkStorage.getNumWalksPerVertex();
						for (int globalWalkId = 0; globalWalkId < numVertexInPartition * numWalksPerVertex;
							 globalWalkId++) {
							int startVertexId = randomWalkStorage.getVertexIdByPosition(globalWalkId, 0);
							int currentEndVertexId = randomWalkStorage.getVertexIdByPosition(globalWalkId,
								superStepId - 1);
							// taskId, vertexId, walkdId, endPoint
							out.collect(
								Tuple4.of(taskId, startVertexId, globalWalkId % numWalksPerVertex,
									currentEndVertexId));
						}
					}
				}
			}).withBroadcastSet(state, "state");

		DataSet <Tuple4 <Integer, Integer, Integer, Integer>> third = localPath.coGroup(neighbours)
			.where(new KeySelector <Tuple4 <Integer, Integer, Integer, Integer>, Integer>() {
				@Override
				public Integer getKey(Tuple4 <Integer, Integer, Integer, Integer> value) throws Exception {
					// taskId, vertexId, walkdId, endPointId
					return value.f3;
				}
			})
			.equalTo(new KeySelector <Tuple2 <Integer, Tuple2 <Integer, Double>[]>, Integer>() {
				@Override
				public Integer getKey(Tuple2 <Integer, Tuple2 <Integer, Double>[]> value) throws Exception {
					return value.f0;
				}
			})
			.with(new SamplingWithWeight()).name("iter_cogroup")
			.partitionCustom(new Partitioner <Integer>() {
				private static final long serialVersionUID = -8435163397805540340L;

				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0);

		DataSet <Integer[]> newState = third.mapPartition(
			new RichMapPartitionFunction <Tuple4 <Integer, Integer, Integer, Integer>, Integer[]>() {
				private static final long serialVersionUID = 4817837060612938072L;

				@Override
				public void mapPartition(Iterable <Tuple4 <Integer, Integer, Integer, Integer>> values,
										 Collector <Integer[]>
											 out)
					throws Exception {
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					int superStepId = getIterationRuntimeContext().getSuperstepNumber();
					RandomWalkStorage randomWalkStorage = IterTaskObjKeeper.get(stateHandler, taskId);

					if (superStepId < walkLength) {
						// update path
						for (Tuple4 <Integer, Integer, Integer, Integer> value : values) {
							// taskId, vertexId, walkId, endPointId
							int vertexId = value.f1;
							int walkId = value.f2;
							int endVertexId = value.f3;
							randomWalkStorage.updatePath(vertexId, walkId, superStepId, endVertexId);
						}
						out.collect(new Integer[1]);
					} else {
						// update the end point and emit the path
						int numVertex = randomWalkStorage.getNumVertex();
						int numWalksPerVertex = randomWalkStorage.getNumWalksPerVertex();
						for (int globalWalkId = 0; globalWalkId < numVertex * numWalksPerVertex; globalWalkId++) {
							out.collect(randomWalkStorage.getWalksById(globalWalkId));
						}
					}
				}
			});

		DataSet <Integer[]> out = state.closeWith(newState);
		IterTaskObjKeeper.clear(stateHandler);
		return out;
	}

	protected static class EdgeNormalize
		implements CoGroupFunction <Edge <Integer, Double>, Edge <Integer, Double>, Edge <Integer, Double>> {
		private static final long serialVersionUID = -6343322555508349419L;

		@Override
		public void coGroup(Iterable <Edge <Integer, Double>> first,
							Iterable <Edge <Integer, Double>> second,
							Collector <Edge <Integer, Double>> out) {
			Edge <Integer, Double> denominator = new Edge <>();
			for (Edge <Integer, Double> i : second) {
				denominator = i;
			}
			double sum = denominator.f2;
			double count = 0.0;
			for (Edge <Integer, Double> i : first) {
				count += i.f2;
				i.f2 = count / sum;

				out.collect(i);
			}
		}
	}

	protected static class SamplingWithoutWeight
		extends
		RichCoGroupFunction <Tuple4 <Integer, Integer, Integer, Integer>,
			Tuple2 <Integer, Integer[]>,
			Tuple4 <Integer, Integer, Integer, Integer>> {
		private static final long serialVersionUID = -1488596752261652510L;
		private Random randSrc;

		public SamplingWithoutWeight() {
			randSrc = new Random();
		}

		/**
		 * in the coGroup step, we need to get all the neighbors from the iterable second.
		 */
		@Override
		public void coGroup(Iterable <Tuple4 <Integer, Integer, Integer, Integer>> first,
							Iterable <Tuple2 <Integer, Integer[]>> second,
							Collector <Tuple4 <Integer, Integer, Integer, Integer>> out) {
			//look for all the neighbors.
			Integer[] neighbours = null;
			for (Tuple2 <Integer, Integer[]> tuple : second) {
				neighbours = tuple.f1;
			}
			if (neighbours == null) {
				// taskId, vertexId, walkId, endPointId
				for (Tuple4 <Integer, Integer, Integer, Integer> tuple : first) {
					tuple.f3 = -1;
					out.collect(tuple);
				}
			} else {
				int dicCnt = neighbours.length;
				for (Tuple4 <Integer, Integer, Integer, Integer> tuple : first) {
					if (tuple.f3 != -1) {
						tuple.f3 = neighbours[randSrc.nextInt(dicCnt)];
					}
					out.collect(tuple);
				}
			}
		}
	}

	//taskId, vertexId, walkdId, endPoint
	protected static class SamplingWithWeight
		extends
		RichCoGroupFunction <Tuple4 <Integer, Integer, Integer, Integer>, Tuple2 <Integer, Tuple2 <Integer, Double>[]>,
			Tuple4 <Integer, Integer, Integer, Integer>> {
		private static final long serialVersionUID = -2312884917638300024L;
		private RandSrc randSrc;

		public SamplingWithWeight() {
			randSrc = new RandSrc();
		}

		/**
		 * in the coGroup step, we need to get all the neighbors from the iterable second and then sort it.
		 */
		@Override
		public void coGroup(Iterable <Tuple4 <Integer, Integer, Integer, Integer>> first,
							Iterable <Tuple2 <Integer, Tuple2 <Integer, Double>[]>> second,
							Collector <Tuple4 <Integer, Integer, Integer, Integer>> out) {
			//look for all the neighbors.
			Tuple2 <Integer, Double>[] neighbor = null;
			for (Tuple2 <Integer, Tuple2 <Integer, Double>[]> i : second) {
				neighbor = i.f1;
			}

			if (neighbor == null) {
				for (Tuple4 <Integer, Integer, Integer, Integer> t1 : first) {
					// taskId, vertexId, walkdId, endPoint
					t1.f3 = -1;
					out.collect(t1);
				}
			} else {
				for (Tuple4 <Integer, Integer, Integer, Integer> t1 : first) {
					if (t1.f3 != -1) {
						t1.f3 = randSrc.run(neighbor);
					}
					out.collect(t1);
				}
			}
		}
	}

	/**
	 * execute weighted sampling, output the chosen targets
	 */
	protected static class RandSrc implements Serializable {
		private static final long serialVersionUID = -1271903427301895811L;
		private Random seed;

		private RandSrc() {
			seed = new Random(2018);
		}

		public Integer run(Tuple2 <Integer, Double>[] dic) {
			double val = seed.nextDouble();
			int idx = Arrays.binarySearch(dic, 0, dic.length - 1, Tuple2.of(-1, val),
				new Comparator <Tuple2 <Integer, Double>>() {
					@Override
					public int compare(Tuple2 <Integer, Double> s1, Tuple2 <Integer, Double> s2) {
						return s1.f1.compareTo(s2.f1);
					}
				});

			return dic[Math.abs(idx) - 1].f0;
		}
	}

	static class RandomWalkStorage implements Serializable {
		int[] walks;
		int numVertex, numWalksPerVertex, walkLen;
		HashMap <Integer, Integer> map;

		public RandomWalkStorage(int numVertex, int numWalksPerVertex, int walkLen) {
			this.numVertex = numVertex;
			this.numWalksPerVertex = numWalksPerVertex;
			this.walkLen = walkLen;
			walks = new int[numVertex * numWalksPerVertex * walkLen];
			map = new HashMap <>();
		}

		public void updatePath(int vertexId, int walkId, int position, int updateValue) {
			int localId = map.get(vertexId);
			int idx = localId * (numWalksPerVertex * walkLen) + walkId * walkLen + position;
			walks[idx] = updateValue;
		}

		// get vertexId at position of a global walkId
		// globalId in [0, numVertex * numWalksPerVertex)
		public int getVertexIdByPosition(int globalWalkId, int position) {
			int idx = globalWalkId * walkLen + position;
			return walks[idx];
		}

		public Integer[] getWalksById(int globalWalkId) {
			Integer[] path = new Integer[walkLen];
			for (int pos = 0; pos < walkLen; pos++) {
				int value = getVertexIdByPosition(globalWalkId, pos);
				if (-1 != value) {
					path[pos] = value;
				} else {
					path[pos] = null;
				}
			}
			return path;
		}

		public void setMap(HashMap <Integer, Integer> map) {
			this.map = map;
		}

		public int getNumVertex() {
			return numVertex;
		}

		public int getNumWalksPerVertex() {
			return numWalksPerVertex;
		}

		public int getWalkLen() {
			return walkLen;
		}
	}
}