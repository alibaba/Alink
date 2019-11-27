package com.alibaba.alink.common.comqueue;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class for the com(Computation && Communicate) queue.
 *
 * @param <Q> the implement of BaseComQueue
 */
public class BaseComQueue<Q extends BaseComQueue<Q>> implements Serializable {

	/**
	 * All computation or communication functions.
	 */
	private final List<ComQueueItem> queue = new ArrayList<>();

	/**
	 * sessionId for shared objects within this BaseComQueue.
	 */
	private final int sessionId = SessionSharedObjs.getNewSessionId();

	/**
	 * The function executed to decide whether to break the loop.
	 */
	private CompareCriterionFunction compareCriterion;

	/**
	 * The function executed when closing the iteration
	 */
	private CompleteResultFunction completeResult;

	/**
	 * Max iteration count.
	 */
	private int maxIter = Integer.MAX_VALUE;

	private transient List<String> cacheDataObjNames = new ArrayList<>();
	private transient DataSet<byte[]> cacheDataRel;
	private transient ExecutionEnvironment executionEnvironment;

	@SuppressWarnings("unchecked")
	private Q thisAsQ() {
		return (Q) this;
	}

	public Q add(ComQueueItem com) {
		queue.add(com);
		return thisAsQ();
	}

	/**
	 * Set the function to decide whether to break the loop.
	 *
	 * If not set, it'll loop till max iteration.
	 *
	 * @param compareCriterion
	 * @return this BaseComQueue itself.
	 */
	protected Q setCompareCriterionOfNode0(CompareCriterionFunction compareCriterion) {
		this.compareCriterion = compareCriterion;
		return thisAsQ();
	}

	/**
	 * Execute the CompleteResultFunction, when closing the iteration.
	 *
	 * @return this BaseComQueue itself.
	 */
	public Q closeWith(CompleteResultFunction completeResult) {
		this.completeResult = completeResult;
		return thisAsQ();
	}

	/**
	 * The maximum iteration number.
	 *
	 * @param maxIter
	 * @return this BaseComQueue itself.
	 */
	protected Q setMaxIter(int maxIter) {
		this.maxIter = maxIter;
		return thisAsQ();
	}

	/**
	 * Add the dataset as shared session object. All workers can get a subset of this dataset as collection via
	 * {@link ComContext#getObj}.
	 *
	 * @param objName object name.
	 * @param data input dataset
	 * @return this BaseComQueue itself.
	 */
	public <T> Q initWithPartitionedData(String objName, DataSet<T> data) {
		createRelationshipAndCachedData(data, objName);
		return thisAsQ();
	}

	/**
	 * Add the dataset as shared session object. All workers can get a full copy of this dataset as collection via
	 * {@link ComContext#getObj}.
	 *
	 * @param objName object name.
	 * @param data input dataset
	 * @return this BaseComQueue itself.
	 */
	public <T> Q initWithBroadcastData(String objName, DataSet<T> data) {
		return initWithPartitionedData(objName, broadcastDataSet(data));
	}

	/**
	 * Set sessionId to run this BaseComQueue.
	 * @param sessionId the session id.
	 * @return this BaseComQueue itself.
	 */
	public Q initWithMLSessionId(Long sessionId) {
		executionEnvironment = MLEnvironmentFactory.get(sessionId).getExecutionEnvironment();
		return thisAsQ();
	}

	/**
	 * Execute the BaseComQueue and get the result dataset.
	 *
	 * @return result dataset.
	 */
	public DataSet<Row> exec() {
		optimize();

		if (executionEnvironment == null) {
			if (cacheDataRel == null) {
				executionEnvironment = MLEnvironmentFactory.getDefault().getExecutionEnvironment();
			} else {
				executionEnvironment = BatchOperator.getExecutionEnvironmentFromDataSets(cacheDataRel);
			}
		}

		IterativeDataSet<byte[]> loop
			= loopStartDataSet(executionEnvironment)
			.iterate(maxIter);

		DataSet<byte[]> input = loop
			.mapPartition(new DistributeData(cacheDataObjNames, sessionId))
			.withBroadcastSet(loop, "barrier")
			.name("distribute data");

		for (ComQueueItem com : queue) {
			if ((com instanceof CommunicateFunction)) {
				CommunicateFunction communication = ((CommunicateFunction) com);
				input = communication.communicateWith(input, sessionId);
			} else if (com instanceof ComputeFunction) {
				final ComputeFunction computation = (ComputeFunction) com;

				input = input
						.mapPartition(new RichMapPartitionFunction<byte[], byte[]>() {

						@Override
						public void mapPartition(Iterable<byte[]> values, Collector<byte[]> out) {
							ComContext context = new ComContext(
								sessionId, getIterationRuntimeContext()
							);
							computation.calc(context);
						}
					})
					.withBroadcastSet(input, "barrier")
					.name(com instanceof ChainedComputation ?
						((ChainedComputation) com).name()
						: "computation@" + computation.getClass().getSimpleName());
			} else {
				throw new RuntimeException("Unsupported op in iterative queue.");
			}
		}

		DataSet<byte[]> loopEnd;
		if (null == compareCriterion) {
			loopEnd = loop.closeWith(
				//completeResult
				input
					.mapPartition(new RichMapPartitionFunction<byte[], byte[]>() {

						@Override
						public void mapPartition(Iterable<byte[]> values, Collector<byte[]> out) throws Exception {
							if (getIterationRuntimeContext().getSuperstepNumber() == maxIter) {
								ComContext context = new ComContext(sessionId, getIterationRuntimeContext());
								List<Row> model = completeResult.calc(context);

								if (null == model) {
									return;
								}

								for (Row m : model) {
									out.collect(SerializationUtils.serialize(m));
								}
							}
						}
					})
					.withBroadcastSet(input, "barrier")
					.name("genNewModel")
			);
		} else {
			// compare Criterion.
			//
			// There are six options with compareCriterion.
			// 1. calculate compareCriterion in single node and calculate completeResult in single node.
			// 2. calculate compareCriterion in single node, broadcast the criterion to all node
			//    and calculate completeResult in all node.
			// 3. calculate compareCriterion in all node and calculate completeResult in single node.
			// 4. calculate compareCriterion in all node and calculate completeResult in all node.
			// 5. calculate compareCriterion in all node, reduce the criterion
			//    and calculate completeResult in single node.
			// 6. calculate compareCriterion in all node, reduce the criterion,
			//    broadcast the reduced criterion to all node and calculate completeResult in all node.
			//
			// we use 2 to implement this function.
			DataSet<Boolean> criterion = input
				.mapPartition(new RichMapPartitionFunction<byte[], Boolean>() {
					@Override
					public void mapPartition(Iterable<byte[]> values, Collector<Boolean> out) throws Exception {
						if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
							ComContext context = new ComContext(sessionId, getIterationRuntimeContext());
							if (!compareCriterion.calc(context)) {
								out.collect(false);
							}
						}
					}
				})
				.withBroadcastSet(input, "barrier")
				.name("genCriterion");

			loopEnd = loop.closeWith(
				//completeResult
				input
					.mapPartition(new RichMapPartitionFunction<byte[], byte[]>() {
						boolean criterion;

						@Override
						public void open(Configuration parameters) {
							criterion = getRuntimeContext()
								.getBroadcastVariableWithInitializer(
									"criterion",
									new BroadcastVariableInitializer<Boolean, Boolean>() {
										@Override
										public Boolean initializeBroadcastVariable(Iterable<Boolean> data) {
											Iterator<Boolean> iterOfData = data.iterator();
											if (iterOfData.hasNext()) {
												return data.iterator().next();
											} else {
												return true;
											}
										}
									}
								);
						}

						@Override
						public void mapPartition(Iterable<byte[]> values, Collector<byte[]> out) {
							ComContext context = new ComContext(sessionId, getIterationRuntimeContext());
							if (getIterationRuntimeContext().getSuperstepNumber() == maxIter
								|| criterion) {
								List<Row> model = completeResult.calc(context);

								if (null == model) {
									return;
								}

								for (Row m : model) {
									out.collect(SerializationUtils.serialize(m));
								}
							}
						}
					})
					.withBroadcastSet(input, "barrier")
					.withBroadcastSet(criterion, "criterion")
					.name("genNewModel")
				,
				criterion
			);
		}

		return serializeModel(clearObjs(loopEnd));
	}

	@Override
	public String toString() {
		Map<String, Object> val = new HashMap<>();
		val.put("queue", queue
			.stream()
			.map(x -> x.getClass().getSimpleName())
			.collect(Collectors.joining(","))
		);
		val.put("sessionId", sessionId);
		val.put("maxIter", maxIter);
		val.put("compareCriterion", compareCriterion == null ? null : compareCriterion.getClass().getSimpleName());
		val.put("completeResult", completeResult == null ? null : completeResult.getClass().getSimpleName());

		return JsonConverter.toJson(val);
	}

	private static DataSet<Row> serializeModel(DataSet<byte[]> model) {
		return model
			.map(new MapFunction<byte[], Row>() {
				@Override
				public Row map(byte[] value) {
					return (Row) SerializationUtils.deserialize(value);
				}
			})
			.name("serializeModel");
	}

	private static <T> DataSet<T> broadcastDataSet(DataSet<T> data) {
		return expandDataSet2MaxParallelism(data)
			.mapPartition(new RichMapPartitionFunction<T, Tuple2<Integer, T>>() {
				@Override
				public void mapPartition(Iterable<T> values, Collector<Tuple2<Integer, T>> out) throws Exception {
					int numTask = getRuntimeContext().getNumberOfParallelSubtasks();
					for (T val : values) {
						for (int i = 0; i < numTask; ++i) {
							out.collect(Tuple2.of(i, val));
						}
					}
				}
			})
			.returns(new TupleTypeInfo<>(Types.INT, data.getType()))
			.name("sharedDataBroadcast")
			.partitionCustom(new Partitioner<Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0)
			.name("sharedDataPartition")
			.mapPartition(new RichMapPartitionFunction<Tuple2<Integer, T>, T>() {
				@Override
				public void mapPartition(Iterable<Tuple2<Integer, T>> values, Collector<T> out) {
					for (Tuple2<Integer, T> val : values) {
						out.collect(val.f1);
					}
				}
			})
			.returns(data.getType())
			.name("sharedDataFly");
	}

	private static <T> DataSet<T> expandDataSet2MaxParallelism(DataSet<T> data) {
		return data
			.map(new RichMapFunction<T, Tuple2<Integer, T>>() {
				@Override
				public Tuple2<Integer, T> map(T value) throws Exception {
					return Tuple2.of(
						getRuntimeContext().getIndexOfThisSubtask(),
						value
					);
				}
			})
			.returns(new TupleTypeInfo<>(Types.INT, data.getType()))
			.name("appendTaskId2Data")
			// use partitionCustom to expand dataset to max parallelism.
			.partitionCustom(new Partitioner<Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0)
			.name("partitionData2Task")
			.map(new MapFunction<Tuple2<Integer, T>, T>() {
				@Override
				public T map(Tuple2<Integer, T> value) {
					return value.f1;
				}
			})
			.returns(data.getType())
			.name("projectData2Raw");
	}

	private DataSet<byte[]> loopStartDataSet(ExecutionEnvironment env) {
		MapPartitionOperator<Integer, byte[]> initial = env
			.fromElements(1)
			.rebalance()
			.mapPartition(new MapPartitionFunction<Integer, byte[]>() {
				@Override
				public void mapPartition(Iterable<Integer> values, Collector<byte[]> out) {
					//pass
				}
			}).name("iterInitialize");

		if (cacheDataRel != null) {
			initial = initial.withBroadcastSet(cacheDataRel, "rel");
		}

		return initial;
	}

	private DataSet<byte[]> clearObjs(DataSet<byte[]> raw) {
		final int localSessionId = sessionId;
		DataSet<byte[]> clear = expandDataSet2MaxParallelism(
			BatchOperator
				.getExecutionEnvironmentFromDataSets(raw)
				.fromElements(0))
			.mapPartition(new MapPartitionFunction<Integer, byte[]>() {
				@Override
				public void mapPartition(Iterable<Integer> values, Collector<byte[]> out) {
					SessionSharedObjs.clear(localSessionId);
				}
			});
		return raw
			.map(new MapFunction<byte[], byte[]>() {
				@Override
				public byte[] map(byte[] value) {
					return value;
				}
			})
			.withBroadcastSet(clear, "barrier")
			.name("clearReturn");

	}

	private <T> void createRelationshipAndCachedData(DataSet<T> data, final String key) {
		final int localSessionId = sessionId;
		if (cacheDataRel == null) {
			cacheDataRel = clearObjs(
				BatchOperator
					.getExecutionEnvironmentFromDataSets(data)
					.fromElements(new byte[0])
					.mapPartition(new MapPartitionFunction<byte[], byte[]>() {
						@Override
						public void mapPartition(Iterable<byte[]> values, Collector<byte[]> out) throws Exception {
							//pass
						}
					})
			);
		}

		DataSet<Tuple2<Integer, Long>> rowCount = DataSetUtils.countElementsPerPartition(data);

		cacheDataRel = data.mapPartition(new PutCachedData<T>(key, localSessionId))
			.withBroadcastSet(cacheDataRel, "rel")
			.withBroadcastSet(rowCount, "rowCount")
			.name("cachedDataRel@" + key);

		cacheDataObjNames.add(key);
	}

	private void optimize() {
		if (queue.isEmpty()) {
			return;
		}

		int current = 0;
		for (int ahead = 1; ahead < queue.size(); ++ahead) {
			ComQueueItem curItem = queue.get(current);
			ComQueueItem aheadItem = queue.get(ahead);

			if (aheadItem instanceof ComputeFunction && curItem instanceof ComputeFunction) {
				if (curItem instanceof ChainedComputation) {
					queue.set(current, ((ChainedComputation) curItem).add((ComputeFunction) aheadItem));
				} else {
					queue.set(current, new ChainedComputation()
						.add((ComputeFunction) curItem)
						.add((ComputeFunction) aheadItem)
					);
				}
			} else {
				queue.set(++current, aheadItem);
			}
		}

		queue.subList(current + 1, queue.size()).clear();
	}

	private static class PutCachedData<T> extends RichMapPartitionFunction<T, byte[]> {
		private final String key;
		private final int sessionId;
		private int localRowCount;

		PutCachedData(String key, int sessionId) {
			this.key = key;
			this.sessionId = sessionId;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			List<Tuple2<Integer, Long>> rowCount = getRuntimeContext().getBroadcastVariable("rowCount");

			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			for (Tuple2<Integer, Long> r : rowCount) {
				if (r.f0.equals(taskId)) {
					localRowCount = r.f1.intValue();
				}
			}
		}

		@Override
		public void mapPartition(Iterable<T> values, Collector<byte[]> out) throws Exception {
			List<T> data = new ArrayList<>(localRowCount);
			values.forEach(data::add);
			SessionSharedObjs.cachePartitionedData(key, sessionId, data);
		}
	}

	private static class DistributeData extends RichMapPartitionFunction<byte[], byte[]> {
		private final List<String> cacheDataObjNames;
		private final int sessionId;

		DistributeData(List<String> cacheDataObjNames, int sessionId) {
			this.cacheDataObjNames = cacheDataObjNames;
			this.sessionId = sessionId;
		}

		@Override
		public void mapPartition(Iterable<byte[]> values, Collector<byte[]> out) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				SessionSharedObjs.distributeCachedData(
					cacheDataObjNames,
					sessionId,
					getRuntimeContext().getIndexOfThisSubtask()
				);
			}
		}
	}
}
