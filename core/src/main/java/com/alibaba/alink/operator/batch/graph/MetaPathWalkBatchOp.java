package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.nlp.walk.MetaPathWalkParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * This algorithm realizes the metaPath Walk.
 * The graph is saved in the form of DataSet of edges,
 * and the output random walk consists of vertices in order.
 * <p>
 * If a walk terminals before reach the walk length, it won't continue and
 * we only need to return this short walk.
 */
public final class MetaPathWalkBatchOp extends BatchOperator <MetaPathWalkBatchOp>
	implements MetaPathWalkParams <MetaPathWalkBatchOp> {

	public static final String PATH_COL_NAME = "path";
	private static final long serialVersionUID = -4645013343119976771L;

	public MetaPathWalkBatchOp() {
		super(new Params());
	}

	public MetaPathWalkBatchOp(Params params) {
		super(params);
	}

	@Override
	public MetaPathWalkBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		BatchOperator <?> in1 = inputs[0];
		BatchOperator <?> in2 = inputs[1];

		Integer walkNum = getWalkNum();
		Integer walkLength = getWalkLength();
		String delimiter = getDelimiter();
		String node0ColName = getSourceCol();
		String node1ColName = getTargetCol();
		String valueColName = getWeightCol();
		Boolean isToUnDigraph = getIsToUndigraph();
		int node0Idx = TableUtil.findColIndexWithAssertAndHint(in1.getColNames(), node0ColName);
		int node1Idx = TableUtil.findColIndexWithAssertAndHint(in1.getColNames(), node1ColName);
		int valueIdx = (valueColName == null) ? -1 : TableUtil.findColIndexWithAssertAndHint(in1.getColNames(),
			valueColName);
		String vertexColName = getVertexCol();
		String typeColName = getTypeCol();
		int vertexIdx = TableUtil.findColIndexWithAssertAndHint(in2.getColNames(), vertexColName);
		int typeIdx = TableUtil.findColIndexWithAssertAndHint(in2.getColNames(), typeColName);
		String metaPath = getMetaPath();
		boolean isWeightedSampling = (valueColName == null || valueColName.isEmpty()) ? false : true;
		String[] metaPaths = metaPath.split(",");
		for (int i = 0; i < metaPaths.length; ++i) {
			metaPaths[i] = metaPaths[i].trim();
		}

		DataSet <Edge <String, Double>>
			edge = WalkUtils.buildEdge(in1.getDataSet(), node0Idx, node1Idx, valueIdx, isToUnDigraph);
		DataSet <Tuple2 <String, Integer>> nodeMapping = WalkUtils.getStringIndexMapping(edge);
		DataSet <Edge <Integer, Double>> initEdge = WalkUtils.string2Index(edge, nodeMapping);

		/* construct node with type */
		final DataSet <Tuple2 <Integer, Character>> node = in2.getDataSet().map(
			new MapFunction <Row, Tuple2 <String, Character>>() {
				private static final long serialVersionUID = 226753315032066559L;

				@Override
				public Tuple2 <String, Character> map(Row row) throws Exception {
					String vertex = row.getField(vertexIdx).toString();
					Character type = ((String) row.getField(typeIdx)).charAt(0);
					return Tuple2.of(vertex, type);
				}
			}).coGroup(nodeMapping).where(0).equalTo(0).with(
			new RichCoGroupFunction <Tuple2 <String, Character>, Tuple2 <String, Integer>, Tuple2 <Integer,
				Character>>() {
				private static final long serialVersionUID = -4731904120133947181L;

				@Override
				public void coGroup(Iterable <Tuple2 <String, Character>> first,
									Iterable <Tuple2 <String, Integer>> second,
									Collector <Tuple2 <Integer, Character>> out) throws Exception {
					Integer node = null;
					for (Tuple2 <String, Integer> t2 : second) {
						node = t2.f1;
					}
					if (node != null) {
						for (Tuple2 <String, Character> e : first) {
							out.collect(Tuple2.of(node, e.f1));
						}
					}
				}
			}).name("cogroup_node_string2int");

		/* construct edge right with node type */
		DataSet <Tuple3 <Integer, Tuple2 <Integer, Character>, Double>>
			edge1WithType = initEdge.coGroup(node)
			.where(1).equalTo(0).with(
				new RichCoGroupFunction <Edge <Integer, Double>, Tuple2 <Integer, Character>, Tuple3 <Integer,
					Tuple2 <Integer, Character>, Double>>() {
					private static final long serialVersionUID = -5083781984737756019L;

					@Override
					public void coGroup(Iterable <Edge <Integer, Double>> edges,
										Iterable <Tuple2 <Integer, Character>> nodes,
										Collector <Tuple3 <Integer, Tuple2 <Integer, Character>, Double>> out)
						throws Exception {
						Character node = null;
						for (Tuple2 <Integer, Character> t2 : nodes) {
							node = t2.f1;
						}
						if (node != null) {
							for (Edge <Integer, Double> edge : edges) {

								out.collect(Tuple3.of(edge.f0, Tuple2.of(edge.f1, node), edge.f2));
							}
						}
					}
				});

		/* construct edge left with node type */
		DataSet <Tuple3 <Tuple2 <Integer, Character>, Integer, Double>>
			edge0WithType = initEdge.coGroup(node).where(0).equalTo(0).with(
			new RichCoGroupFunction <Edge <Integer, Double>, Tuple2 <Integer, Character>, Tuple3 <Tuple2 <Integer,
				Character>,
				Integer, Double>>() {
				private static final long serialVersionUID = -6152308307872186565L;

				@Override
				public void coGroup(Iterable <Edge <Integer, Double>> edges,
									Iterable <Tuple2 <Integer, Character>> nodes,
									Collector <Tuple3 <Tuple2 <Integer, Character>, Integer, Double>> out)
					throws Exception {
					Character node = null;
					for (Tuple2 <Integer, Character> t2 : nodes) {
						node = t2.f1;
					}
					if (node != null) {
						for (Edge <Integer, Double> edge : edges) {

							out.collect(Tuple3.of(Tuple2.of(edge.f0, node), edge.f1, edge.f2));
						}
					}
				}
			});

		/* random walk with metaPath */
		DataSet <Integer[]> path =
			isWeightedSampling ? weightedSampling(edge0WithType, edge1WithType, nodeMapping, walkNum, walkLength,
				metaPaths) :
				randomSampling(edge0WithType, edge1WithType, nodeMapping, walkNum, walkLength, metaPaths);

		DataSet <String[]> out = WalkUtils.index2String(path, nodeMapping, walkLength);
		setOutputTable(WalkUtils.transString(out, new String[] {PATH_COL_NAME}, delimiter, getMLEnvironmentId()));
		return this;
	}

	private DataSet <Integer[]> randomSampling(
		DataSet <Tuple3 <Tuple2 <Integer, Character>, Integer, Double>> edge0WithType,
		DataSet <Tuple3 <Integer, Tuple2 <Integer, Character>, Double>> edge1WithType,
		DataSet <Tuple2 <String, Integer>> nodeMapping,
		int walkNum,
		int walkLength,
		final String[] metaPaths) {

		/* construct neighbors of every node */
		DataSet <Tuple2 <Integer, Map <Character, List <Integer>>>>
			neighbours = edge1WithType.groupBy(0).reduceGroup(
			new GroupReduceFunction <Tuple3 <Integer, Tuple2 <Integer, Character>, Double>,
				Tuple2 <Integer, Map <Character, List <Integer>>>>() {
				private static final long serialVersionUID = 2156066724541299383L;

				@Override
				public void reduce(
					Iterable <Tuple3 <Integer, Tuple2 <Integer, Character>, Double>> values,
					Collector <Tuple2 <Integer, Map <Character, List <Integer>>>> out)
					throws Exception {
					Integer left = null;
					Map <Character, List <Integer>> neighbours = new HashMap <>(0);
					boolean first = true;
					for (Tuple3 <Integer, Tuple2 <Integer, Character>, Double> e : values) {
						if (first) {
							left = e.f0;
							first = false;
						}
						if (neighbours.containsKey(e.f1.f1)) {
							neighbours.get(e.f1.f1).add(e.f1.f0);
						} else {
							List <Integer> neighbour = new ArrayList <>();
							neighbour.add(e.f1.f0);
							neighbours.put(e.f1.f1, neighbour);
						}
					}
					out.collect(Tuple2.of(left, neighbours));
				}
			});

		/**
		 * then write the start point into the arrays.
		 * Tuple3: StartNode, MetaPath, path
		 */
		DataSet <Tuple2 <String, Integer[]>> path = edge0WithType
			.filter(
				new FilterFunction <Tuple3 <Tuple2 <Integer, Character>, Integer, Double>>() {
					private static final long serialVersionUID = 6034985627961192240L;

					@Override
					public boolean filter(Tuple3 <Tuple2 <Integer, Character>, Integer, Double> t3)
						throws Exception {
						for (int i = 0; i < metaPaths.length; ++i) {
							if (t3.f0.f1 == metaPaths[i].charAt(0)) {
								return true;
							}
						}
						return false;
					}
				})
			.distinct(0)
			.mapPartition(new InitialMap(walkNum, walkLength, metaPaths));

		DataSet <Integer> broadcastVar = nodeMapping.mapPartition(
			new RichMapPartitionFunction <Tuple2 <String, Integer>, Integer>() {
				private static final long serialVersionUID = -1160705555930115133L;

				@Override
				public void mapPartition(Iterable <Tuple2 <String, Integer>> values, Collector <Integer> out)
					throws Exception {
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						out.collect(1);
					}
				}
			});

		/* construct state of iteration */
		IterativeDataSet <Integer[]> state = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment()
			.fromElements(1).map(new MapFunction <Integer, Integer[]>() {
				private static final long serialVersionUID = -3466588946761656087L;

				@Override
				public Integer[] map(Integer value) throws Exception {
					return new Integer[1];
				}
			}).withBroadcastSet(broadcastVar, "broadVar").iterate(walkLength);

		final long stateHandler = IterTaskObjKeeper.getNewHandle();

		/* Tuple4 : taskId, MetaPath, startNode, currentNode */
		DataSet <Tuple3 <Integer, String, Integer>> localPath = getLocalPath(path, state, stateHandler);
		/* Tuple4 : taskId, MetaPath, startNode, forwardNode */
		DataSet <Tuple3 <Integer, String, Integer>> third = localPath.coGroup(neighbours)
			.where(new SelectKeyFormer())
			.equalTo(new SelectKeyLatterWithoutWeight())
			.with(new RandomSampling()).name("iter_cogroup")
			.partitionCustom(new Partitioner <Integer>() {
				private static final long serialVersionUID = 4212110668677476836L;

				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0);

		/* update new State by local path */
		DataSet <Integer[]> newState = updateState(third, stateHandler, walkLength);

		DataSet <Integer[]> out = state.closeWith(newState);
		IterTaskObjKeeper.clear(stateHandler);
		return out;
	}

	private DataSet <Integer[]> weightedSampling(
		DataSet <Tuple3 <Tuple2 <Integer, Character>, Integer, Double>> edge0WithType,
		DataSet <Tuple3 <Integer, Tuple2 <Integer, Character>, Double>> edge1WithType,
		DataSet <Tuple2 <String, Integer>> nodeMapping,
		int walkNum,
		int walkLength,
		final String[] metaPaths) {
		/* construct neighbors of every node */
		DataSet <Tuple2 <Integer, Tuple2 <Character, Tuple2 <Integer, Double>[]>>>
			neighbours = edge1WithType.groupBy(0).reduceGroup(
			new GroupReduceFunction <Tuple3 <Integer, Tuple2 <Integer, Character>, Double>,
				Tuple2 <Integer, Map <Character, List <Tuple2 <Integer, Double>>>>>() {

				private static final long serialVersionUID = -2366618719301937344L;

				@Override
				public void reduce(Iterable <Tuple3 <Integer, Tuple2 <Integer, Character>, Double>> values,
								   Collector <Tuple2 <Integer, Map <Character, List <Tuple2 <Integer, Double>>>>> out)
					throws Exception {
					List <Tuple3 <Integer, Tuple2 <Integer, Character>, Double>> ret = new ArrayList <>(0);
					Map <Character, double[]> sums = new HashMap <>(0);
					for (Tuple3 <Integer, Tuple2 <Integer, Character>, Double> ele : values) {
						if (sums.containsKey(ele.f1.f1)) {
							sums.get(ele.f1.f1)[0] += ele.f2;
						} else {
							sums.put(ele.f1.f1, new double[] {ele.f2});
						}
						ele.f2 = sums.get(ele.f1.f1)[0];
						ret.add(ele);
					}

					for (Tuple3 <Integer, Tuple2 <Integer, Character>, Double> ele : ret) {
						ele.f2 /= sums.get(ele.f1.f1)[0];
					}

					Integer left = null;
					Map <Character, List <Tuple2 <Integer, Double>>> neighbours = new HashMap <>(0);
					boolean first = true;
					for (Tuple3 <Integer, Tuple2 <Integer, Character>, Double> e : ret) {
						if (first) {
							left = e.f0;
							first = false;
						}
						if (neighbours.containsKey(e.f1.f1)) {
							neighbours.get(e.f1.f1).add(Tuple2.of(e.f1.f0, e.f2));
						} else {
							List <Tuple2 <Integer, Double>> neighbour = new ArrayList <>();
							neighbour.add(Tuple2.of(e.f1.f0, e.f2));
							neighbours.put(e.f1.f1, neighbour);
						}
					}
					out.collect(Tuple2.of(left, neighbours));
				}
			}).flatMap(
			new FlatMapFunction <Tuple2 <Integer, Map <Character, List <Tuple2 <Integer, Double>>>>, Tuple2 <Integer,
				Tuple2 <Character, Tuple2 <Integer, Double>[]>>>() {
				private static final long serialVersionUID = -723668678350346753L;

				@Override
				public void flatMap(Tuple2 <Integer, Map <Character, List <Tuple2 <Integer, Double>>>> value,
									Collector <Tuple2 <Integer, Tuple2 <Character, Tuple2 <Integer, Double>[]>>> out)
					throws Exception {
					final int SIZE = 20000;
					for (Character chara : value.f1.keySet()) {
						List <Tuple2 <Integer, Double>> list = value.f1.get(chara);
						if (list.size() > SIZE) {
							int collectSize = list.size() / SIZE;
							for (int i = 0; i < collectSize; ++i) {
								Tuple2 <Integer, Double>[] neighbors = new Tuple2[SIZE];
								for (int j = 0; j < SIZE; ++j) {
									neighbors[j] = list.get(i * SIZE + j);
								}
								out.collect(Tuple2.of(value.f0, Tuple2.of(chara, neighbors)));
							}
							int leftSize = list.size() % SIZE;
							Tuple2 <Integer, Double>[] neighbors = new Tuple2[leftSize];
							for (int j = 0; j < leftSize; ++j) {
								neighbors[j] = list.get(collectSize * SIZE + j);
							}
							out.collect(Tuple2.of(value.f0, Tuple2.of(chara, neighbors)));
						} else {
							Tuple2 <Integer, Double>[] neighbors = new Tuple2[list.size()];
							for (int i = 0; i < neighbors.length; ++i) {
								neighbors[i] = list.get(i);
							}
							out.collect(Tuple2.of(value.f0, Tuple2.of(chara, neighbors)));
						}
					}
				}
			});

		/**
		 * then write the start point into the arrays.
		 * Tuple3: StartNode, MetaPath, path
		 */
		DataSet <Tuple2 <String, Integer[]>> path = edge0WithType
			.filter(
				new FilterFunction <Tuple3 <Tuple2 <Integer, Character>, Integer, Double>>() {
					private static final long serialVersionUID = -2839052759232259899L;

					@Override
					public boolean filter(Tuple3 <Tuple2 <Integer, Character>, Integer, Double> t3)
						throws Exception {
						for (int i = 0; i < metaPaths.length; ++i) {
							if (t3.f0.f1 == metaPaths[i].charAt(0)) {
								return true;
							}
						}
						return false;
					}
				})
			.distinct(0)
			.mapPartition(new InitialMap(walkNum, walkLength, metaPaths));

		DataSet <Integer> broadcastVar = nodeMapping.mapPartition(
			new RichMapPartitionFunction <Tuple2 <String, Integer>, Integer>() {
				private static final long serialVersionUID = 8351561691743725167L;

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
				private static final long serialVersionUID = 4182523518453372899L;

				@Override
				public Integer[] map(Integer value) throws Exception {
					return new Integer[1];
				}
			}).withBroadcastSet(broadcastVar, "broadVar").iterate(walkLength);

		final long stateHandler = IterTaskObjKeeper.getNewHandle();

		/* Tuple4 : taskId, MetaPath, startNode, currentNode */
		DataSet <Tuple3 <Integer, String, Integer>> localPath = getLocalPath(path, state, stateHandler);
		/* Tuple4 : taskId, MetaPath, startNode, forwardNode */
		DataSet <Tuple3 <Integer, String, Integer>> third = localPath.coGroup(neighbours)
			.where(new SelectKeyFormer())
			.equalTo(new SelectKeyLatterWithWeight())
			.with(new WeightedSampling()).name("iter_cogroup")
			.partitionCustom(new Partitioner <Integer>() {
				private static final long serialVersionUID = 2926197091531494593L;

				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0);

		DataSet <Integer[]> newState = updateState(third, stateHandler, walkLength);
		DataSet <Integer[]> out = state.closeWith(newState);
		IterTaskObjKeeper.clear(stateHandler);
		return out;
	}

	DataSet <Tuple3 <Integer, String, Integer>> getLocalPath(DataSet <Tuple2 <String, Integer[]>> path,
															 IterativeDataSet <Integer[]> state,
															 final long stateHandler) {
		DataSet <Tuple3 <Integer, String, Integer>> localPath = path
			.mapPartition(
				new RichMapPartitionFunction <Tuple2 <String, Integer[]>, Tuple3 <Integer, String,
					Integer>>() {
					private static final long serialVersionUID = -6216487264288311779L;

					@Override
					public void mapPartition(Iterable <Tuple2 <String, Integer[]>> values,
											 Collector <Tuple3 <Integer, String, Integer>> out)
						throws Exception {
						int taskId = getRuntimeContext().getIndexOfThisSubtask();
						if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
							List <Tuple2 <String, Integer[]>> localState = new ArrayList <>();
							for (Tuple2 <String, Integer[]> t2 : values) {
								localState.add(t2);
								out.collect(
									Tuple3.of(getRuntimeContext().getIndexOfThisSubtask(), t2.f0, t2.f1[0]));
							}
							IterTaskObjKeeper.put(stateHandler, taskId, localState);
						} else {
							List <Tuple2 <String, Integer[]>> localState = IterTaskObjKeeper.get(stateHandler,
								taskId);
							for (Tuple2 <String, Integer[]> t3 : localState) {
								int idx = getIterationRuntimeContext().getSuperstepNumber() - 1;

								out.collect(Tuple3.of(getRuntimeContext().getIndexOfThisSubtask(), t3.f0,
									(t3.f1[idx] != null) ? t3.f1[idx] : -1));
							}
						}
					}
				}).withBroadcastSet(state, "state");
		return localPath;
	}

	DataSet <Integer[]> updateState(DataSet <Tuple3 <Integer, String, Integer>> third, final long stateHandler,
									final int walkLength) {
		return third.mapPartition(
			new RichMapPartitionFunction <Tuple3 <Integer, String, Integer>, Integer[]>() {
				private static final long serialVersionUID = 5659302167710724581L;

				@Override
				public void mapPartition(Iterable <Tuple3 <Integer, String, Integer>> values,
										 Collector <Integer[]> out)
					throws Exception {
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					List <Tuple2 <String, Integer[]>> localState = IterTaskObjKeeper.get(stateHandler,
						taskId);
					if (getIterationRuntimeContext().getSuperstepNumber() < walkLength) {
						// update path
						Map <String, Integer> nodeMap = new HashMap <>(0);
						for (Tuple3 <Integer, String, Integer> t2 : values) {
							nodeMap.put(t2.f1, t2.f2);
						}
						for (Tuple2 <String, Integer[]> t2 : localState) {
							Integer node = nodeMap.get(t2.f0);
							if (node != -1) {
								t2.f1[getIterationRuntimeContext().getSuperstepNumber()] = node;
							}
						}
						out.collect(new Integer[1]);
					} else {

						for (Tuple2 <String, Integer[]> t2 : localState) {
							out.collect(t2.f1);
						}
					}
				}
			});

	}

	protected static class WeightedSampling extends
		RichCoGroupFunction <Tuple3 <Integer, String, Integer>,
			Tuple2 <Integer, Tuple2 <Character, Tuple2 <Integer, Double>[]>>, Tuple3 <Integer, String, Integer>> {

		private static final long serialVersionUID = 1882571426241944564L;
		private Random rand;

		public WeightedSampling() {
			rand = new Random(2019);

		}

		/**
		 * in the coGroup step, we need to get all the neighbors from the iterable second and then sort it.
		 */
		@Override
		public void coGroup(Iterable <Tuple3 <Integer, String, Integer>> first,
							Iterable <Tuple2 <Integer, Tuple2 <Character, Tuple2 <Integer, Double>[]>>> second,
							Collector <Tuple3 <Integer, String, Integer>> out) throws Exception {
			//look for all the neighbors.
			int step = getIterationRuntimeContext().getSuperstepNumber();
			Map <Character, List <Tuple2 <Integer, Double>[]>> tmpNeighbours = new HashMap <>();
			Map <Character, Tuple2 <Integer, Double>[]> neighbours = new HashMap <>();
			for (Tuple2 <Integer, Tuple2 <Character, Tuple2 <Integer, Double>[]>> ele : second) {
				if (tmpNeighbours.containsKey(ele.f1.f0)) {
					tmpNeighbours.get(ele.f1.f0).add(ele.f1.f1);
				} else {
					List <Tuple2 <Integer, Double>[]> list = new ArrayList <>();
					list.add(ele.f1.f1);
					tmpNeighbours.put(ele.f1.f0, list);
				}
			}

			for (Character key : tmpNeighbours.keySet()) {
				List <Tuple2 <Integer, Double>[]> list = tmpNeighbours.get(key);
				int size = 0;
				for (Tuple2 <Integer, Double>[] ele : list) {
					size += ele.length;
				}
				Tuple2 <Integer, Double>[] neis = new Tuple2[size];
				int iter = 0;
				for (Tuple2 <Integer, Double>[] ele : list) {
					for (int i = 0; i < ele.length; ++i) {
						neis[iter++] = ele[i];
					}
				}
				neighbours.put(key, neis);
			}

			if (neighbours == null) {
				for (Tuple3 <Integer, String, Integer> t2 : first) {
					t2.f2 = -1;
					out.collect(t2);
				}
			} else {
				for (Tuple3 <Integer, String, Integer> t2 : first) {
					if (t2.f2 != -1) {
						int pos = t2.f1.indexOf("$");
						String metaPath = t2.f1.substring(pos + 1, t2.f1.length());
						char currentType = metaPath.charAt((step - 1) % metaPath.length());

						Tuple2 <Integer, Double>[] aNeighbours = neighbours.get(currentType);
						if (aNeighbours == null) {
							t2.f2 = -1;
							out.collect(t2);
						} else {
							t2.f2 = WalkUtils.weightSample(aNeighbours, rand);
						}
					}
					out.collect(t2);
				}
			}
		}
	}

	protected static class InitialMap
		implements
		MapPartitionFunction <Tuple3 <Tuple2 <Integer, Character>, Integer, Double>, Tuple2 <String, Integer[]>> {
		private static final long serialVersionUID = -5512217778019957179L;
		private Integer walkNum;
		private int walkLength;
		private String[] metaPaths;

		private InitialMap(Integer walkNum, int walkLength, String[] metaPaths) {
			this.walkNum = walkNum;
			this.walkLength = walkLength;
			this.metaPaths = metaPaths;
		}

		@Override
		public void mapPartition(
			Iterable <Tuple3 <Tuple2 <Integer, Character>, Integer, Double>> values,
			Collector <Tuple2 <String, Integer[]>> out) throws Exception {
			for (Tuple3 <Tuple2 <Integer, Character>, Integer, Double> edge : values) {
				for (int i = 0; i < walkNum; i++) {
					Integer[] temp = new Integer[walkLength];
					for (int j = 0; j < walkLength; j++) {
						temp[j] = null;
					}
					temp[0] = edge.f0.f0;
					for (int m = 0; m < metaPaths.length; ++m) {
						if (metaPaths[m].charAt(0) == edge.f0.f1) {
							out.collect(Tuple2.of(edge.f0.f0 + "_" + i + "$" + metaPaths[m].substring(1), temp));
						}
					}
				}
			}
		}
	}

	protected static class SelectKeyFormer implements KeySelector <Tuple3 <Integer, String, Integer>, Integer> {
		private static final long serialVersionUID = 2760025979971105302L;

		@Override
		public Integer getKey(Tuple3 <Integer, String, Integer> value) throws Exception {
			return value.f2;
		}
	}

	protected static class SelectKeyLatterWithoutWeight
		implements KeySelector <Tuple2 <Integer, Map <Character, List <Integer>>>, Integer> {
		private static final long serialVersionUID = 6114619244203151268L;

		@Override
		public Integer getKey(Tuple2 <Integer, Map <Character, List <Integer>>> value) throws Exception {
			return value.f0;
		}
	}

	protected static class SelectKeyLatterWithWeight
		implements KeySelector <Tuple2 <Integer, Tuple2 <Character, Tuple2 <Integer, Double>[]>>, Integer> {
		private static final long serialVersionUID = 8098370537479008223L;

		@Override
		public Integer getKey(Tuple2 <Integer, Tuple2 <Character, Tuple2 <Integer, Double>[]>> value) throws
			Exception {
			return value.f0;
		}
	}

	protected static class RandomSampling
		extends
		RichCoGroupFunction <Tuple3 <Integer, String, Integer>, Tuple2 <Integer, Map <Character, List <Integer>>>,
			Tuple3 <Integer, String, Integer>> {
		private static final long serialVersionUID = -1358623098559450730L;
		private Random randSrc;

		public RandomSampling() {
			randSrc = new Random();
		}

		@Override
		public void coGroup(Iterable <Tuple3 <Integer, String, Integer>> first,
							Iterable <Tuple2 <Integer, Map <Character, List <Integer>>>> second,
							Collector <Tuple3 <Integer, String, Integer>> out) {
			//look for all the neighbors.
			int step = getIterationRuntimeContext().getSuperstepNumber();

			Map <Character, List <Integer>> neighbours = null;
			for (Tuple2 <Integer, Map <Character, List <Integer>>> t2 : second) {
				neighbours = t2.f1;
			}

			if (neighbours == null) {
				for (Tuple3 <Integer, String, Integer> t2 : first) {
					t2.f2 = -1;
					out.collect(t2);
				}
			} else {

				for (Tuple3 <Integer, String, Integer> t2 : first) {
					if (t2.f2 != -1) {
						int pos = t2.f1.indexOf("$");
						String metaPath = t2.f1.substring(pos + 1, t2.f1.length());

						char currentType = metaPath.charAt((step - 1) % metaPath.length());
						List <Integer> aNeighbours = neighbours.get(currentType);
						if (aNeighbours == null) {
							t2.f2 = -1;
							out.collect(t2);
						} else {
							int dicCnt = aNeighbours.size();
							t2.f2 = aNeighbours.get(randSrc.nextInt(dicCnt));
						}
					}
					out.collect(t2);
				}
			}
		}
	}
}