package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.SplitParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_128;

/**
 * Split a dataset into two parts.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@NameCn("数据拆分")
public final class SplitBatchOp extends BatchOperator <SplitBatchOp>
	implements SplitParams <SplitBatchOp> {

	private static final long serialVersionUID = -1436970192619749693L;

	public SplitBatchOp() {
		this(new Params());
	}

	public SplitBatchOp(Params params) {
		super(params);
	}

	public SplitBatchOp(double fraction) {
		this(new Params().set(FRACTION, fraction));
	}

	@Override
	public SplitBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final double fraction = getFraction();
		if (fraction < 0. || fraction > 1.0) {
			throw new RuntimeException("invalid fraction " + fraction);
		}

		DataSet <Row> rows = in.getDataSet();

		Integer seed = getRandomSeed();
		DataSet <Tuple2 <Boolean, Row>> out;

		if (null != seed) {
			final HashFunction hashFunc = murmur3_128(seed);

			DataSet <Tuple2 <Long, Row>> hashValue = rows.map(new RichMapFunction <Row, Tuple2 <Long, Row>>() {
				private static final long serialVersionUID = -287601103797809499L;

				@Override
				public Tuple2 <Long, Row> map(Row value) throws Exception {
					Long hashValue = hashFunc.hashUnencodedChars(RowUtil.rowToString(value)).asLong();
					return Tuple2.of(hashValue, value);
				}
			}).partitionCustom(new Partitioner <Long>() {
				private static final long serialVersionUID = 871499283994717282L;

				@Override
				public int partition(Long key, int numPartitions) {
					return Math.abs(key.intValue()) % numPartitions;
				}
			}, 0);

			DataSet <Tuple2 <Integer, Long>> countsPerPartition = DataSetUtils.countElementsPerPartition(hashValue);

			DataSet <long[]> numPickedPerPartition = countsPerPartition
				.mapPartition(new CountInPartition(fraction))
				.setParallelism(1)
				.name("decide_count_of_each_partition");

			out = hashValue
				.sortPartition(0, Order.ASCENDING)
				.mapPartition(new PickInPartitionWithSeed(seed))
				.withBroadcastSet(numPickedPerPartition, "counts")
				.name("pick_in_each_partition");

		} else {
			// tuple: partition no., count in partition
			DataSet <Tuple2 <Integer, Long>> countsPerPartition = DataSetUtils.countElementsPerPartition(rows);

			DataSet <long[]> numPickedPerPartition = countsPerPartition
				.mapPartition(new CountInPartition(fraction))
				.setParallelism(1)
				.name("decide_count_of_each_partition");

			out = rows
				.mapPartition(new PickInPartition())
				.withBroadcastSet(numPickedPerPartition, "counts")
				.name("pick_in_each_partition");
		}

		DataSet <Row> left = out.flatMap(new FlatMapFunction <Tuple2 <Boolean, Row>, Row>() {
			private static final long serialVersionUID = -1015919192379666607L;

			@Override
			public void flatMap(Tuple2 <Boolean, Row> value, Collector <Row> out) throws Exception {
				if (value.f0) {
					out.collect(value.f1);
				}
			}
		});

		DataSet <Row> right = out.flatMap(new FlatMapFunction <Tuple2 <Boolean, Row>, Row>() {
			private static final long serialVersionUID = -7288487577579174535L;

			@Override
			public void flatMap(Tuple2 <Boolean, Row> value, Collector <Row> out) throws Exception {
				if (!value.f0) {
					out.collect(value.f1);
				}
			}
		});

		this.setOutput(left, in.getSchema());
		this.setSideOutputTables(
			new Table[] {DataSetConversionUtil.toTable(getMLEnvironmentId(), right, in.getSchema())});
		return this;
	}

	/**
	 * Randomly decide the number of elements to select in each task
	 */
	private static class CountInPartition extends RichMapPartitionFunction <Tuple2 <Integer, Long>, long[]> {
		private static final long serialVersionUID = 7797942238612563554L;
		private double fraction;

		public CountInPartition(double fraction) {
			this.fraction = fraction;
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, Long>> values, Collector <long[]> out) throws Exception {
			Preconditions.checkArgument(getRuntimeContext().getIndexOfThisSubtask() == 0);

			long totCount = 0L;
			List <Tuple2 <Integer, Long>> buffer = new ArrayList <>();
			for (Tuple2 <Integer, Long> value : values) {
				totCount += value.f1;
				buffer.add(value);
			}

			int npart = buffer.size(); // num tasks
			long[] eachCount = new long[npart];
			long numTarget = Math.round((totCount * fraction));
			long[] eachSelect = new long[npart];

			for (Tuple2 <Integer, Long> value : buffer) {
				eachCount[value.f0] = value.f1;
			}

			long totSelect = 0L;
			for (int i = 0; i < npart; i++) {
				eachSelect[i] = Math.round(Math.floor(eachCount[i] * fraction));
				totSelect += eachSelect[i];
			}

			if (totSelect < numTarget) {
				long remain = numTarget - totSelect;
				remain = Math.min(remain, totCount - totSelect);
				if (remain == totCount - totSelect) {
					for (int i = 0; i < npart; i++) {
						eachSelect[i] = eachCount[i];
					}
				} else {
					// select 'remain' out of 'npart'
					List <Integer> shuffle = new ArrayList <>(npart);
					while (remain > 0) {
						for (int i = 0; i < npart; i++) {
							shuffle.add(i);
						}
						Collections.shuffle(shuffle, new Random(0L));
						for (int i = 0; i < Math.min(remain, npart); i++) {
							int taskId = shuffle.get(i);
							while (eachSelect[taskId] >= eachCount[taskId]) {
								taskId = (taskId + 1) % npart;
							}
							eachSelect[taskId]++;
						}
						remain -= npart;
					}
				}
			}

			long[] statistics = new long[npart * 2];
			for (int i = 0; i < npart; i++) {
				statistics[i] = eachCount[i];
				statistics[i + npart] = eachSelect[i];
			}
			out.collect(statistics);

		}
	}

	/**
	 * Randomly pick elements in each task
	 */
	private static class PickInPartition extends RichMapPartitionFunction <Row, Tuple2 <Boolean, Row>> {
		private static final long serialVersionUID = 2835501123999397324L;

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Boolean, Row>> out)
			throws Exception {

			int npart = getRuntimeContext().getNumberOfParallelSubtasks();
			List <long[]> bc = getRuntimeContext().getBroadcastVariable("counts");
			long[] eachCount = Arrays.copyOfRange(bc.get(0), 0, npart);
			long[] eachSelect = Arrays.copyOfRange(bc.get(0), npart, npart * 2);

			if (bc.get(0).length / 2 != getRuntimeContext().getNumberOfParallelSubtasks()) {
				throw new RuntimeException("parallelism has changed");
			}

			int taskId = getRuntimeContext().getIndexOfThisSubtask();

			// emit the selected
			int[] selected = null;
			int iRow = 0;
			int numEmits = 0;
			for (Row row : values) {
				if (0 == iRow) {
					long count = eachCount[taskId];
					long select = eachSelect[taskId];

					List <Integer> shuffle = new ArrayList <>((int) count);
					for (int i = 0; i < count; i++) {
						shuffle.add(i);
					}
					Collections.shuffle(shuffle, new Random(taskId));

					selected = new int[(int) select];
					for (int i = 0; i < select; i++) {
						selected[i] = shuffle.get(i);
					}
					Arrays.sort(selected);
				}

				if (numEmits < selected.length && iRow == selected[numEmits]) {
					out.collect(Tuple2.of(true, row));
					numEmits++;
				} else {
					out.collect(Tuple2.of(false, row));
				}
				iRow++;
			}
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(SplitBatchOp.class);

	/**
	 * Randomly pick elements in each task
	 */
	private static class PickInPartitionWithSeed extends
		RichMapPartitionFunction <Tuple2 <Long, Row>, Tuple2 <Boolean, Row>> {
		private static final long serialVersionUID = 2835501123999397324L;
		private int seed;

		public PickInPartitionWithSeed(int seed) {
			this.seed = seed;
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Long, Row>> values, Collector <Tuple2 <Boolean, Row>> out)
			throws Exception {

			long[] bc = (long[]) getRuntimeContext().getBroadcastVariable("counts").get(0);
			LOG.info(Arrays.toString(bc));
			long[] eachCount = Arrays.copyOfRange(bc, 0, bc.length / 2);
			long[] eachSelect = Arrays.copyOfRange(bc, bc.length / 2, bc.length);

			int group = getRuntimeContext().getIndexOfThisSubtask();
			if (bc.length / 2 != getRuntimeContext().getNumberOfParallelSubtasks()) {
				throw new RuntimeException("parallelism has changed");
			}
			int count = (int) eachCount[group];
			int select = (int) eachSelect[group];

			List <Integer> indices = new ArrayList <>();

			for (int i = 0; i < count; i++) {
				indices.add(i);
			}
			Collections.shuffle(indices, new Random(seed));
			indices = indices.subList(0, select);
			Collections.sort(indices);

			Comparator <Row> comparator = new Comparator <Row>() {
				@Override
				public int compare(Row o1, Row o2) {
					int res;
					for (int i = 0; i < o1.getArity(); i++) {
						if (o1.getField(i) instanceof Comparable) {
							res = ((Comparable) o1.getField(i)).compareTo(o2.getField(i));
						} else {
							res = o1.getField(i).toString().compareTo(o2.getField(i).toString());
						}
						if (res != 0) {
							return res;
						}
					}
					return 0;
				}
			};

			int cnt = 0;
			Tuple2 <Long, Row> pre = null;
			int curIndex = 0;
			Integer curValue = select == 0 ? null : indices.get(curIndex);

			for (Tuple2 <Long, Row> t : values) {
				if (null == curValue) {
					out.collect(Tuple2.of(false, t.f1));
					cnt++;
					continue;
				}
				if (pre == null || pre.f0 < t.f0 || comparator.compare(pre.f1, t.f1) <= 0) {
					if (cnt - 1 == curValue) {
						out.collect(Tuple2.of(true, pre.f1));
						curIndex++;
						curValue = curIndex >= select ? null : indices.get(curIndex);
					} else if (pre != null) {
						out.collect(Tuple2.of(false, pre.f1));
					}
					pre = t;
				} else if (pre.f0 > t.f0) {
					throw new RuntimeException("Order error!");
				} else {
					if (cnt - 1 == curValue) {
						out.collect(Tuple2.of(true, t.f1));
						curIndex++;
						curValue = curIndex >= select ? null : indices.get(curIndex);
					} else {
						out.collect(Tuple2.of(false, t.f1));
					}
				}
				cnt++;
			}
			Preconditions.checkArgument(cnt == (int) count, "Group value not equal to count value!");
			if (null != curValue) {
				Preconditions.checkArgument(cnt - 1 == curValue && curIndex + 1 == select,
					"Inner error, select number not equal to index!");
				out.collect(Tuple2.of(true, pre.f1));
			} else if (null != pre) {
				out.collect(Tuple2.of(false, pre.f1));
			}
		}
	}
}
