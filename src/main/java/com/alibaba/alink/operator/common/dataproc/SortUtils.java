package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SortUtils {

	public final static int SPLIT_POINT_SIZE = 1000;

	/**
	 * <p>
	 * reference: Yang, X. (2014). Chong gou da shu ju tong ji (1st ed., pp. 25-29).
	 * <p>
	 * Note: This algorithm is improved on the base of the parallel
	 * sorting by regular sampling(PSRS).
	 *
	 * @param input input dataset
	 * @param index the index of input row which need to sort.
	 * @return f0: dataset which is indexed by partition id, f1: dataset which has partition id and count.
	 */
	public static Tuple2 <DataSet <Tuple2 <Integer, Row>>, DataSet <Tuple2 <Integer, Long>>> pSort(
		DataSet <Row> input, int index) {

		DataSet <Tuple2 <Object, Integer>> splitPoints = input
 			.mapPartition(new SampleSplitPoint(index))
			.reduceGroup(new SplitPointReducer());

		DataSet <Tuple2 <Integer, Row>> splitData = input
			.mapPartition(new SplitData(index))
			.withBroadcastSet(splitPoints, "splitPoints");

		DataSet <Tuple2 <Integer, Long>> allCount = splitData
			.groupBy(0)
			.withPartitioner(new AvgPartition())
			.reduceGroup(new GroupReduceFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, Long>>() {
				@Override
				public void reduce(
					Iterable <Tuple2 <Integer, Row>> values,
					Collector <Tuple2 <Integer, Long>> out) {
					Integer id = -1;
					Long count = 0L;
					for (Tuple2 <Integer, Row> value : values) {
						id = value.f0;
						count++;
					}

					out.collect(new Tuple2 <>(id, count));
				}
			});

		return new Tuple2 <>(splitData, allCount);
	}

	public static Long genSampleIndex(Long splitPointIdx, Long count, Long splitPointSize) {
		splitPointIdx++;
		splitPointSize++;

		Long div = count / splitPointSize;
		Long mod = count % splitPointSize;

		return div * splitPointIdx + ((mod > splitPointIdx) ? splitPointIdx : mod) - 1;
	}

	/**
	 *
	 */
	public static class SampleSplitPoint extends RichMapPartitionFunction <Row, Tuple2 <Object, Integer>> {
		private int taskId;
		private int index;

		public SampleSplitPoint(int index) {
			this.index = index;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			this.taskId = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Object, Integer>> out) throws Exception {
			ArrayList <Object> allValues = new ArrayList <>();
			for (Row row : values) {
				allValues.add(row.getField(index));
			}

			if (allValues.isEmpty()) {
				return;
			}

			Collections.sort(allValues, new ComparableComparator());

			int size = allValues.size();

			ArrayList <Object> splitPoints = new ArrayList <>();

			int localSplitPointSize = Math.min(SPLIT_POINT_SIZE, size - 1);

			for (int i = 0; i < localSplitPointSize; ++i) {
				int index = genSampleIndex(
					Long.valueOf(i),
					Long.valueOf(size),
					Long.valueOf(localSplitPointSize)
				).intValue();

				if (index >= size) {
					throw new Exception("Index error. index: " + index + ". totalCount: " + size);
				}

				splitPoints.add(allValues.get(index));
			}

			for (Object obj : splitPoints) {
				Tuple2 <Object, Integer> cur
					= new Tuple2 <Object, Integer>(
					obj,
					taskId);

				out.collect(cur);
			}

			out.collect(new Tuple2(
				getRuntimeContext().getNumberOfParallelSubtasks(),
				-taskId - 1));
		}
	}

	public static class SplitPointReducer
		extends RichGroupReduceFunction <Tuple2 <Object, Integer>, Tuple2 <Object, Integer>> {

		public SplitPointReducer() {
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
		}

		@Override
		public void reduce(
			Iterable <Tuple2 <Object, Integer>> values,
			Collector <Tuple2 <Object, Integer>> out) throws Exception {
			ArrayList <Tuple2 <Object, Integer>> all = new ArrayList <>();
			int instanceCount = -1;

			for (Tuple2 <Object, Integer> value : values) {
				if (value.f1 < 0) {
					instanceCount = (int) value.f0;
					continue;
				}
				all.add(new Tuple2 <>(value.f0, value.f1));
			}

			if (all.isEmpty()) {
				return;
			}

			int count = all.size();

			Collections.sort(all, new PairComparator());

			Set <Tuple2 <Object, Integer>> spliters = new HashSet <>();

			int splitPointSize = instanceCount - 1;
			for (int i = 0; i < splitPointSize; ++i) {
				int index = genSampleIndex(
					Long.valueOf(i),
					Long.valueOf(count),
					Long.valueOf(splitPointSize))
					.intValue();

				if (index >= count) {
					throw new Exception("Index error. index: " + index + ". totalCount: " + count);
				}

				spliters.add(all.get(index));
			}

			for (Tuple2 <Object, Integer> spliter : spliters) {
				out.collect(spliter);
			}
		}
	}

	public static class SplitData extends RichMapPartitionFunction <Row, Tuple2 <Integer, Row>> {
		private int taskId;
		private List <Tuple2 <Object, Integer>> splitPoints;
		private int index;

		public SplitData(int index) {
			this.index = index;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			RuntimeContext ctx = getRuntimeContext();

			this.taskId = ctx.getIndexOfThisSubtask();
			this.splitPoints = ctx.getBroadcastVariableWithInitializer(
				"splitPoints",
				new BroadcastVariableInitializer <Tuple2 <Object, Integer>, List <Tuple2 <Object, Integer>>>() {
					@Override
					public List <Tuple2 <Object, Integer>> initializeBroadcastVariable(
						Iterable <Tuple2 <Object, Integer>> data) {
						// sort the list by task id to calculate the correct offset
						List <Tuple2 <Object, Integer>> sortedData = new ArrayList <>();
						for (Tuple2 <Object, Integer> datum : data) {
							sortedData.add(datum);
						}
						Collections.sort(sortedData, new PairComparator());
						return sortedData;
					}
				});
		}

		/**
		 * use binary search to partition data into sorted subsets
		 * notice: data within each subset will not be sorted.
		 */
		@Override
		public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, Row>> out) throws Exception {
			if (splitPoints.isEmpty()) {
				for (Row row : values) {
					out.collect(new Tuple2 <>(0, row));
				}

				return;
			}

			for (Row row : values) {
				Tuple2 <Object, Integer> curTuple = new Tuple2 <>(
					null,
					taskId
				);
				curTuple.f0 = row.getField(index);
				int bsIndex = Collections.binarySearch(splitPoints, curTuple, new PairComparator());

				int curIndex;
				if (bsIndex >= 0) {
					curIndex = bsIndex;
				} else {
					curIndex = -bsIndex - 1;
				}

				out.collect(new Tuple2 <>(curIndex, row));
			}
		}
	}

	public static class ComparableComparator implements Comparator <Object> {
		@Override
		public int compare(Object o1, Object o2) {
			if (o1 == null && o2 == null) {
				return 0;
			} else if (o1 == null) {
				return 1;
			} else if (o2 == null) {
				return -1;
			}

			Comparable c1 = (Comparable) o1;
			Comparable c2 = (Comparable) o2;

			return c1.compareTo(c2);
		}
	}

	public static class PairComparator
		implements Comparator <Tuple2 <Object, Integer>> {
		ComparableComparator objectComparator = new ComparableComparator();

		@Override
		public int compare(Tuple2 <Object, Integer> o1, Tuple2 <Object, Integer> o2) {
			int ret = objectComparator.compare(o1.f0, o2.f0);
			if (ret == 0) {
				return o1.f1.compareTo(o2.f1);
			}

			return ret;
		}
	}

	public static class AvgPartition implements Partitioner <Integer> {

		@Override
		public int partition(Integer key, int numPartitions) {
			return key % numPartitions;
		}
	}

	public static class AvgLongPartitioner implements Partitioner <Long> {

		@Override
		public int partition(Long key, int numPartitions) {
			return (int) (key % numPartitions);
		}
	}

	public static class RowComparator implements Comparator <Row> {
		private ComparableComparator objectComparator = new ComparableComparator();
		private int index;

		public RowComparator(int index) {
			this.index = index;
		}

		@Override
		public int compare(Row o1, Row o2) {
			return objectComparator.compare(
				o1.getField(index),
				o2.getField(index));
		}
	}
}
