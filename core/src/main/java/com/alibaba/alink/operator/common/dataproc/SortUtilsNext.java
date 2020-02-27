package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class SortUtilsNext {
	private static final Logger LOG = LoggerFactory.getLogger(SortUtilsNext.class);

	private final static int SPLIT_POINT_SIZE = 1000;

	/**
	 * <p>
	 * reference: Yang, X. (2014). Chong gou da shu ju tong ji (1st ed., pp. 25-29).
	 * <p>
	 * Note: This algorithm is improved on the base of the parallel
	 * sorting by regular sampling(PSRS).
	 *
	 * @param input input dataset
	 * @return f0: dataset which is indexed by partition id, f1: dataset which has partition id and count.
	 */
	public static <T extends Comparable<T>> Tuple2<DataSet<T>, DataSet<Tuple2<Integer, Long>>>
	pSort(DataSet<T> input) {

		DataSet<Tuple2<Integer, Long>> cnt = DataSetUtils.countElementsPerPartition(input);

		DataSet<T> sorted = input
			.mapPartition(new RichMapPartitionFunction<T, T>() {
				int taskId;
				int cnt;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);

					taskId = getRuntimeContext().getIndexOfThisSubtask();

					LOG.info("{} open.", getRuntimeContext().getTaskName());

					List<Tuple2<Integer, Long>> cntVar = getRuntimeContext().getBroadcastVariable("cnt");

					for (Tuple2<Integer, Long> var : cntVar) {
						if (var.f0 == taskId) {
							cnt = var.f1.intValue();
							break;
						}
					}
				}

				@Override
				public void close() throws Exception {
					super.close();

					LOG.info("{} close.", getRuntimeContext().getTaskName());
				}

				@Override
				public void mapPartition(Iterable<T> values, Collector<T> out) throws Exception {
					ArrayList<T> all = new ArrayList<>(cnt);

					for (T val : values) {
						all.add(val);
					}

					all.sort(Comparator.naturalOrder());

					for (T val : all) {
						out.collect(val);
					}
				}
			})
			.withBroadcastSet(cnt, "cnt")
			.returns(input.getType());

		DataSet<Tuple2<Object, Integer>> splitPoints = sorted
			.mapPartition(new SampleSplitPoint<>())
			.withBroadcastSet(cnt, "cnt")
			.reduceGroup(new SplitPointReducer());

		DataSet<Tuple2<Integer, T>> splitData = sorted
			.mapPartition(new SplitData<>())
			.withBroadcastSet(splitPoints, "splitPoints")
			.returns(new TupleTypeInfo<>(Types.INT, input.getType()));

		DataSet<T> partitioned = splitData
			.partitionCustom(new Partitioner<Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0)
			.map(new MapFunction<Tuple2<Integer, T>, T>() {
				@Override
				public T map(Tuple2<Integer, T> value) throws Exception {
					return value.f1;
				}
			})
			.returns(input.getType());

		DataSet<Tuple2<Integer, Long>> partitionedCnt = DataSetUtils.countElementsPerPartition(partitioned);

		return Tuple2.of(partitioned, partitionedCnt);
	}

	private static long genSampleIndex(long splitPointIdx, long count, long splitPointSize) {
		splitPointIdx++;
		splitPointSize++;

		long div = count / splitPointSize;
		long mod = count % splitPointSize;

		return div * splitPointIdx + (Math.min(mod, splitPointIdx)) - 1;
	}

	/**
	 *
	 */
	public final static class SampleSplitPoint<T> extends RichMapPartitionFunction<T, Tuple2<Object, Integer>> {
		private int taskId;
		private int cnt;

		public SampleSplitPoint() {
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			this.taskId = getRuntimeContext().getIndexOfThisSubtask();

			LOG.info("{} open.", getRuntimeContext().getTaskName());

			List<Tuple2<Integer, Long>> allCnt = getRuntimeContext().getBroadcastVariable("cnt");

			for (Tuple2<Integer, Long> localCnt : allCnt) {
				if (localCnt.f0 == taskId) {
					cnt = localCnt.f1.intValue();
					break;
				}
			}
		}

		@Override
		public void close() throws Exception {
			super.close();

			LOG.info("{} close.", getRuntimeContext().getTaskName());
		}

		@Override
		public void mapPartition(Iterable<T> values, Collector<Tuple2<Object, Integer>> out) throws Exception {

			if (cnt <= 0) {
				out.collect(new Tuple2(
					getRuntimeContext().getNumberOfParallelSubtasks(),
					-taskId - 1));
				return;
			}

			int localSplitPointSize = Math.min(SPLIT_POINT_SIZE, cnt - 1);

			ArrayList<Tuple2<Object, Integer>> splitPoints
				= new ArrayList<>(localSplitPointSize);

			int dataIndex = 0;
			int splitPointIdx = 0;
			int splitPointInDataIndex = (int) genSampleIndex(splitPointIdx, cnt, localSplitPointSize);
			for (T val : values) {
				if (dataIndex == splitPointInDataIndex) {
					out.collect(Tuple2.of(val, taskId));

					splitPointInDataIndex = (int) genSampleIndex(++splitPointIdx, cnt, localSplitPointSize);
					if (splitPointInDataIndex >= cnt) {
						break;
					}
				}
				dataIndex++;
			}

			out.collect(new Tuple2(
				getRuntimeContext().getNumberOfParallelSubtasks(),
				-taskId - 1));
		}
	}

	public static class SplitPointReducer
		extends RichGroupReduceFunction<Tuple2<Object, Integer>, Tuple2<Object, Integer>> {

		public SplitPointReducer() {
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			LOG.info("{} open.", getRuntimeContext().getTaskName());
		}

		@Override
		public void close() throws Exception {
			super.close();
			LOG.info("{} close.", getRuntimeContext().getTaskName());
		}

		@Override
		public void reduce(
			Iterable<Tuple2<Object, Integer>> values,
			Collector<Tuple2<Object, Integer>> out) throws Exception {
			ArrayList<Tuple2<Object, Integer>> all = new ArrayList<>();
			int instanceCount = -1;

			for (Tuple2<Object, Integer> value : values) {
				if (value.f1 < 0) {
					instanceCount = (int) value.f0;
					continue;
				}
				all.add(Tuple2.of(value.f0, value.f1));
			}

			if (all.isEmpty()) {
				return;
			}

			int count = all.size();

			all.sort(new SortUtils.PairComparator());

			Set<Tuple2<Object, Integer>> split = new HashSet<>();

			int splitPointSize = instanceCount - 1;
			for (int i = 0; i < splitPointSize; ++i) {
				int index = (int) genSampleIndex(i, count, splitPointSize);

				if (index >= count) {
					throw new Exception("Index error. index: " + index + ". totalCount: " + count);
				}

				split.add(all.get(index));
			}

			for (Tuple2<Object, Integer> sSplit : split) {
				out.collect(sSplit);
			}
		}
	}

	public final static class SplitData<T> extends RichMapPartitionFunction<T, Tuple2<Integer, T>> {
		private int taskId;
		private List<Tuple2<Object, Integer>> splitPoints;
		private Tuple2<Integer, T> outBuff;

		public SplitData() {
		}

		@Override
		public void close() throws Exception {
			super.close();
			LOG.info("{} close.", getRuntimeContext().getTaskName());
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			RuntimeContext ctx = getRuntimeContext();

			this.taskId = ctx.getIndexOfThisSubtask();
			this.splitPoints = ctx.getBroadcastVariableWithInitializer(
				"splitPoints",
				new BroadcastVariableInitializer<Tuple2<Object, Integer>, List<Tuple2<Object, Integer>>>() {
					@Override
					public List<Tuple2<Object, Integer>> initializeBroadcastVariable(
						Iterable<Tuple2<Object, Integer>> data) {
						// sort the list by task id to calculate the correct offset
						List<Tuple2<Object, Integer>> sortedData = new ArrayList<>();
						for (Tuple2<Object, Integer> datum : data) {
							sortedData.add(datum);
						}
						sortedData.sort(new SortUtils.PairComparator());
						return sortedData;
					}
				});
			outBuff = new Tuple2<>();
			LOG.info("{} open.", getRuntimeContext().getTaskName());
		}

		/**
		 * use binary search to partition data into sorted subsets
		 * notice: data within each subset will not be sorted.
		 */
		@Override
		public void mapPartition(Iterable<T> values, Collector<Tuple2<Integer, T>> out) throws Exception {
			if (splitPoints.isEmpty()) {
				for (T val : values) {
					outBuff.setFields(0, val);
					out.collect(outBuff);
				}

				return;
			}

			int splitSize = splitPoints.size();
			int curIndex = 0;
			SortUtils.PairComparator pairComparator = new SortUtils.PairComparator();
			Tuple2<Object, Integer> curTuple = Tuple2.of(null, taskId);

			for (T val : values) {
				if (curIndex < splitSize) {
					curTuple.f0 = val;
					int code = pairComparator.compare(
						curTuple,
						splitPoints.get(curIndex)
					);

					if (code > 0) {
						++curIndex;

						while (curIndex < splitSize) {
							code = pairComparator.compare(
								curTuple,
								splitPoints.get(curIndex));

							if (code <= 0) {
								break;
							}

							++curIndex;
						}
					}
				}

				outBuff.setFields(curIndex, val);
				out.collect(outBuff);
			}
		}
	}
}
