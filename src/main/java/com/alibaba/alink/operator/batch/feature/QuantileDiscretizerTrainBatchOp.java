package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.feature.QuantileDiscretizerTrainParams;
import com.alibaba.alink.params.statistics.HasRoundMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * Fit a quantile discretizer model.
 */
public final class QuantileDiscretizerTrainBatchOp extends BatchOperator<QuantileDiscretizerTrainBatchOp>
	implements QuantileDiscretizerTrainParams <QuantileDiscretizerTrainBatchOp> {

	private static final Logger LOG = LoggerFactory.getLogger(QuantileDiscretizerTrainBatchOp.class);

	public QuantileDiscretizerTrainBatchOp() {
	}

	public QuantileDiscretizerTrainBatchOp(Params params) {
		super(params);
	}

	public static DataSet <Row> quantile(DataSet <Row> input, final int[] quantileNum, final String roundMode) {

        /* instance count of dataset */
		DataSet <Long> cnt = DataSetUtils.countElementsPerPartition(input)
			.sum(1).map(new MapFunction <Tuple2 <Integer, Long>, Long>() {
				@Override
				public Long map(Tuple2 <Integer, Long> value) throws Exception {
					return value.f1;
				}
			});

        /* missing count of columns */
		DataSet <Tuple2 <Integer, Long>> missingCount = input
			.mapPartition(new RichMapPartitionFunction <Row, Tuple2 <Integer, Long>>() {
				@Override
				public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, Long>> out)
					throws Exception {
					StreamSupport.stream(values.spliterator(), false)
						.flatMap(x -> {
							long[] counts = new long[x.getArity()];

							Arrays.fill(counts, 0L);

							for (int i = 0; i < x.getArity(); ++i) {
								if (x.getField(i) == null) {
									counts[i]++;
								}
							}

							return IntStream.range(0, x.getArity())
								.mapToObj(y -> Tuple2.of(y, counts[y]));
						})
						.collect(Collectors.groupingBy(
							x -> x.f0,
							Collectors.mapping(x -> x.f1, Collectors.reducing((a, b) -> a + b))
							)
						)
						.entrySet()
						.stream()
						.map(x -> Tuple2.of(x.getKey(), x.getValue().get()))
						.forEach(out::collect);
				}
			})
			.groupBy(0)
			.reduce(new ReduceFunction <Tuple2 <Integer, Long>>() {
				@Override
				public Tuple2 <Integer, Long> reduce(Tuple2 <Integer, Long> value1, Tuple2 <Integer, Long> value2)
					throws Exception {
					return Tuple2.of(value1.f0, value1.f1 + value2.f1);
				}
			});

        /* flatten dataset to 1d */
		DataSet <Row> flatten = input.flatMap(
			new FlatMapFunction <Row, Row>() {
				@Override
				public void flatMap(Row value, Collector <Row> out) throws Exception {
					for (int i = 0; i < value.getArity(); ++i) {
						out.collect(Row.of(new PairComparable <>(i,
								value.getField(i) == null ? null : (Number) value.getField(i))
							)
						);
					}
				}
			});

        /* sort data */
		Tuple2 <DataSet <Tuple2 <Integer, Row>>, DataSet <Tuple2 <Integer, Long>>> sortedData
			= SortUtils.pSort(flatten, 0);

        /* calculate quantile */
		DataSet <Row> quantile = sortedData.f0
			.groupBy(0)
			.withPartitioner(new AvgPartitioner())
			.reduceGroup(new MultiQuantile(0, quantileNum, roundMode))
			.withBroadcastSet(sortedData.f1, "counts")
			.withBroadcastSet(cnt, "totalCnt")
			.withBroadcastSet(missingCount, "missingCounts")
			.distinct(0, 1)
			.groupBy(0)
			.reduceGroup(new RichGroupReduceFunction <Tuple2 <Integer, Double>, Row>() {
				@Override
				public void reduce(Iterable <Tuple2 <Integer, Double>> values, Collector <Row> out) throws Exception {
					ArrayList <Double> l = new ArrayList <>();
					int id = -1;
					for (Tuple2 <Integer, Double> val : values) {
						id = val.getField(0);
						l.add(val.getField(1));
					}

					Collections.sort(l);

					out.collect(Row.of(id, l.stream().mapToDouble(Double::doubleValue).toArray()));
				}
			});

		return quantile;
	}

	@Override
	public QuantileDiscretizerTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
		BatchOperator<?> in = checkAndGetFirst(inputs);
		if (getParams().contains(QuantileDiscretizerTrainParams.NUM_BUCKETS) && getParams().contains(
			QuantileDiscretizerTrainParams.NUM_BUCKETS_ARRAY)) {
			throw new RuntimeException("It can not set num_buckets and num_buckets_array at the same time.");
		}

		String[] quantileColNames =
			getSelectedCols();

		int[] quantileNum = null;

		if (getParams().contains(QuantileDiscretizerTrainParams.NUM_BUCKETS)) {
			quantileNum = new int[quantileColNames.length];
			Arrays.fill(quantileNum, getNumBuckets());
		} else {
			quantileNum = Arrays.stream(getNumBucketsArray()).mapToInt(Integer::intValue).toArray();
		}

        /* filter the selected column from input */
		DataSet <Row> input = in.select(quantileColNames).getDataSet();

		DataSet <Row> quantile = quantile(input, quantileNum, getParams().get(HasRoundMode.ROUND_MODE));

		quantile = quantile
			.map(new RichMapFunction <Row, Row>() {
				String[] quantileColNames;

				@Override
				public void open(Configuration parameters) throws Exception {
					quantileColNames = getRuntimeContext().getBroadcastVariable("quantileColNames").toArray(
						new String[0]);
				}

				@Override
				public Row map(Row value) throws Exception {
					return Row.of(quantileColNames[(int) value.getField(0)], value.getField(1));
				}
			})
			.withBroadcastSet(MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment()
					.fromElements(quantileColNames),
				"quantileColNames"
			)
			.reduceGroup(new SerializeModel(getParams()));

        /* set output */
		setOutput(quantile, new QuantileDiscretizerModelDataConverter().getModelSchema());

		return this;
	}

	public enum RoundModeEnum implements RoundType {
		/**
		 * ⌈a⌉
		 */
		CEIL(new RoundType() {
			@Override
			public long calc(double a) {
				return (long) Math.ceil(a);
			}
		}),

		/**
		 * ⌊a⌋
		 */
		FLOOR(new RoundType() {
			@Override
			public long calc(double a) {
				return (long) Math.floor(a);
			}
		}),

		/**
		 * [a]
		 */
		ROUND(new RoundType() {
			@Override
			public long calc(double a) {
				return Math.round(a);
			}
		});

		private final RoundType roundType;

		RoundModeEnum(RoundType roundType) {
			this.roundType = roundType;
		}

		@Override
		public String toString() {
			return super.name();
		}

		@Override
		public long calc(double a) {
			/**
			 * 0.1 * (8.0 - 1.0) * 10.0 = 7.000000000000001,
			 * we hold 14 digits after the decimal point to avoid this situation
			 */
			BigDecimal bigDecimal = new BigDecimal(a);
			return roundType.calc(bigDecimal.setScale(14,
				BigDecimal.ROUND_HALF_UP).doubleValue());
		}
	}

	public interface RoundType {
		long calc(double a);
	}

	public static class MultiQuantile
		extends RichGroupReduceFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, Double>> {
		private int index;
		private List <Tuple2 <Integer, Long>> counts;
		private List <Tuple2 <Integer, Long>> missingCounts;
		private long totalCnt = 0;
		private int[] quantileNum;
		private String roundType;

		public MultiQuantile(int index, int[] quantileNum, String roundType) {
			this.index = index;
			this.quantileNum = quantileNum;
			this.roundType = roundType;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.counts = getRuntimeContext().getBroadcastVariableWithInitializer(
				"counts",
				new BroadcastVariableInitializer <Tuple2 <Integer, Long>, List <Tuple2 <Integer, Long>>>() {
					@Override
					public List <Tuple2 <Integer, Long>> initializeBroadcastVariable(
						Iterable <Tuple2 <Integer, Long>> data) {
						List <Tuple2 <Integer, Long>> sortedData = new ArrayList <>();
						for (Tuple2 <Integer, Long> datum : data) {
							sortedData.add(datum);
						}
						Collections.sort(sortedData, Comparator.comparing(o -> o.f0));

						return sortedData;
					}
				});

			this.totalCnt = getRuntimeContext().getBroadcastVariableWithInitializer("totalCnt",
				new BroadcastVariableInitializer <Long, Long>() {
					@Override
					public Long initializeBroadcastVariable(Iterable <Long> data) {
						return data.iterator().next();
					}
				});

			this.missingCounts = getRuntimeContext().getBroadcastVariableWithInitializer(
				"missingCounts",
				new BroadcastVariableInitializer <Tuple2 <Integer, Long>, List <Tuple2 <Integer, Long>>>() {
					@Override
					public List <Tuple2 <Integer, Long>> initializeBroadcastVariable(
						Iterable <Tuple2 <Integer, Long>> data) {
						return StreamSupport.stream(data.spliterator(), false)
							.sorted(Comparator.comparing(o -> o.f0))
							.collect(Collectors.toList());
					}
				}
			);
		}

		@Override
		public void reduce(Iterable <Tuple2 <Integer, Row>> values, Collector <Tuple2 <Integer, Double>> out)
			throws Exception {
			ArrayList <Row> allRows = new ArrayList <>();
			int id = -1;

			for (Tuple2 <Integer, Row> value : values) {
				id = value.f0;
				allRows.add(Row.copy(value.f1));
			}

			if (allRows.isEmpty()) {
				return;
			}

			LOG.info("taskId: {}, size: {}", getRuntimeContext().getIndexOfThisSubtask(), allRows.size());

			if (id < 0) {
				throw new RuntimeException("Error key. key: " + id);
			}

			Collections.sort(allRows, new SortUtils.RowComparator(this.index));

			long start = 0;
			long end;

			int curListIndex = -1;
			int size = counts.size();

			for (int i = 0; i < size; ++i) {
				int curId = counts.get(i).f0;

				if (curId == id) {
					curListIndex = i;
					break;
				}

				if (curId > id) {
					throw new RuntimeException("Error curId: " + curId
						+ ". id: " + id);
				}

				start += counts.get(i).f1;
			}

			end = start + counts.get(curListIndex).f1;

			if (allRows.size() != end - start) {
				throw new Exception("Error start end."
					+ " start: " + start
					+ ". end: " + end
					+ ". size: " + allRows.size());
			}

			size = (int) ((end - 1) / totalCnt - start / totalCnt) + 1;

			int localStart = 0;

			for (int i = 0; i < size; ++i) {
				int fIdx = (int) (start / totalCnt + i);
				int subStart = 0;
				int subEnd = (int) totalCnt;

				if (i == 0) {
					subStart = (int) (start % totalCnt);
				}

				if (i == size - 1) {
					subEnd = (int) (end % totalCnt == 0 ? totalCnt : end % totalCnt);
				}

				QIndex qIndex = new QIndex(
					totalCnt - missingCounts.get(fIdx).f1, quantileNum[fIdx], roundType);

				for (int j = 1; j < quantileNum[fIdx]; ++j) {
					long index = qIndex.genIndex(j);

					if (index >= subStart && index < subEnd) {
						PairComparable pairComparable = (PairComparable) allRows.get(
							(int) (index + localStart - subStart)).getField(0);
						out.collect(new Tuple2(pairComparable.first, pairComparable.second.doubleValue()));
					}
				}

				localStart += subEnd - subStart;
			}
		}

	}

	public static class PairComparable<T0 extends Comparable, T1 extends Number>
		implements Comparable <PairComparable> {
		public static final SortUtils.ComparableComparator OBJECT_COMPARATOR = new SortUtils.ComparableComparator();
		public T0 first;
		public T1 second;

		public PairComparable(T0 first, T1 second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public int compareTo(PairComparable o) {
			int f = this.first.compareTo(o.first);

			return f == 0 ? OBJECT_COMPARATOR.compare(this.second, o.second) : f;
		}
	}

	public static class SerializeModel implements GroupReduceFunction <Row, Row> {
		private Params meta;

		public SerializeModel(Params meta) {
			this.meta = meta;
		}

		@Override
		public void reduce(Iterable <Row> values, Collector <Row> out) throws Exception {
			Map <String, double[]> m = new HashMap <>();
			for (Row val : values) {
				m.put((String) val.getField(0), (double[]) val.getField(1));
			}

			QuantileDiscretizerModelDataConverter model = new QuantileDiscretizerModelDataConverter(m, meta);

			model.save(model, out);
		}
	}

	public static class AvgPartitioner implements Partitioner <Integer> {
		@Override
		public int partition(Integer key, int numPartitions) {
			return key % numPartitions;
		}
	}

	public static class QIndex {
		private double totalCount;
		private double q1;
		private RoundModeEnum roundMode;

		public QIndex(double totalCount, int quantileNum, String type) {
			this.totalCount = totalCount;
			this.q1 = 1.0 / (double) quantileNum;
			this.roundMode = RoundModeEnum.valueOf(type.trim().toUpperCase());
		}

		public long genIndex(int k) {
			return roundMode.calc(this.q1 * (this.totalCount - 1.0) * (double) k);
		}
	}
}
