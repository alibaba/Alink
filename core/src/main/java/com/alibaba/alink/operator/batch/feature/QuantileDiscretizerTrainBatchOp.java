package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamCond;
import com.alibaba.alink.common.annotation.ParamCond.CondType;
import com.alibaba.alink.common.annotation.ParamCond.CondValue;
import com.alibaba.alink.common.annotation.ParamCond.CondValueType;
import com.alibaba.alink.common.annotation.ParamMutexRule;
import com.alibaba.alink.common.annotation.ParamMutexRule.ActionType;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.operator.common.dataproc.SortUtilsNext;
import com.alibaba.alink.operator.common.feature.ContinuousRanges;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelInfo;
import com.alibaba.alink.operator.common.feature.binning.BinDivideType;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculatorTransformer;
import com.alibaba.alink.operator.common.feature.quantile.PairComparable;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.params.feature.QuantileDiscretizerTrainParams;
import com.alibaba.alink.params.statistics.HasRoundMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * Fit a quantile discretizer model.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamMutexRule(
	name = "numBuckets",
	type = ActionType.DISABLE,
	cond = @ParamCond(
		name = "numBucketsArray",
		type = CondType.WHEN_VALUES_NOT_IN,
		values = {@CondValue(type = CondValueType.NULL), @CondValue("[]")}
	)
)
@ParamMutexRule(
	name = "numBucketsArray",
	type = ActionType.DISABLE,
	cond = @ParamCond(
		name = "numBuckets",
		type = CondType.WHEN_VALUES_NOT_IN,
		values = {@CondValue(type = CondValueType.NULL), @CondValue()}
	)
)
@NameCn("分位数离散化训练")
public final class QuantileDiscretizerTrainBatchOp extends BatchOperator <QuantileDiscretizerTrainBatchOp>
	implements QuantileDiscretizerTrainParams <QuantileDiscretizerTrainBatchOp>,
	WithModelInfoBatchOp <QuantileDiscretizerModelInfo, QuantileDiscretizerTrainBatchOp,
		QuantileDiscretizerModelInfoBatchOp> {

	private static final Logger LOG = LoggerFactory.getLogger(QuantileDiscretizerTrainBatchOp.class);
	private static final long serialVersionUID = -3670323265976188058L;

	public QuantileDiscretizerTrainBatchOp() {
	}

	public QuantileDiscretizerTrainBatchOp(Params params) {
		super(params);
	}

	public static DataSet <Row> quantile(
		DataSet <Row> input,
		final int[] quantileNum,
		final HasRoundMode.RoundMode roundMode,
		final boolean zeroAsMissing) {

		Tuple4 <DataSet <PairComparable>, DataSet <Tuple2 <Integer, Long>>,
			DataSet <Long>, DataSet <Tuple2 <Integer, Long>>> quantileData =
			quantilePreparing(input, zeroAsMissing);

		/* calculate quantile */
		return quantileData.f0
			.mapPartition(new MultiQuantile(quantileNum, roundMode))
			.withBroadcastSet(quantileData.f1, "counts")
			.withBroadcastSet(quantileData.f2, "totalCnt")
			.withBroadcastSet(quantileData.f3, "missingCounts")
			.groupBy(0)
			.reduceGroup(new ReduceQuantile());
	}

	public static Tuple4 <DataSet <PairComparable>, DataSet <Tuple2 <Integer, Long>>,
		DataSet <Long>, DataSet <Tuple2 <Integer, Long>>> quantilePreparing(DataSet <Row> input,
																			final boolean zeroAsMissing) {
		/* instance count of dataset */
		DataSet <Long> cnt = DataSetUtils
			.countElementsPerPartition(input)
			.sum(1)
			.map(new MapFunction <Tuple2 <Integer, Long>, Long>() {
				private static final long serialVersionUID = 9186222895920287915L;

				@Override
				public Long map(Tuple2 <Integer, Long> value) throws Exception {
					return value.f1;
				}
			})
			.name("calc_cnt");

		/* missing count of columns */
		DataSet <Tuple2 <Integer, Long>> missingCount = input
			.mapPartition(new RichMapPartitionFunction <Row, Tuple2 <Integer, Long>>() {
				private static final long serialVersionUID = -5500000914692866092L;

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
				public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, Long>> out)
					throws Exception {
					StreamSupport.stream(values.spliterator(), false)
						.flatMap(x -> {
							long[] counts = new long[x.getArity()];

							Arrays.fill(counts, 0L);

							for (int i = 0; i < x.getArity(); ++i) {
								if (x.getField(i) == null
									|| (zeroAsMissing && ((Number) x.getField(i)).doubleValue() == 0.0)
									|| Double.isNaN(((Number) x.getField(i)).doubleValue())) {
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
			.name("missingCount")
			.groupBy(0)
			.reduce(new RichReduceFunction <Tuple2 <Integer, Long>>() {
				private static final long serialVersionUID = -4641176463754046550L;

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
				public Tuple2 <Integer, Long> reduce(Tuple2 <Integer, Long> value1, Tuple2 <Integer, Long> value2)
					throws Exception {
					return Tuple2.of(value1.f0, value1.f1 + value2.f1);
				}
			})
			.name("missingCount_reduce");

		/* flatten dataset to 1d */
		DataSet <PairComparable> flatten = input
			.mapPartition(new RichMapPartitionFunction <Row, PairComparable>() {

				private static final long serialVersionUID = 4276686914588972879L;
				PairComparable pairBuff;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					LOG.info("{} open.", getRuntimeContext().getTaskName());
					pairBuff = new PairComparable();
				}

				@Override
				public void close() throws Exception {
					super.close();
					LOG.info("{} close.", getRuntimeContext().getTaskName());
				}

				@Override
				public void mapPartition(Iterable <Row> values, Collector <PairComparable> out) {
					for (Row value : values) {
						for (int i = 0; i < value.getArity(); ++i) {
							pairBuff.first = i;
							if (value.getField(i) == null
								|| (zeroAsMissing && ((Number) value.getField(i)).doubleValue() == 0.0)
								|| Double.isNaN(((Number) value.getField(i)).doubleValue())) {
								pairBuff.second = null;
							} else {
								pairBuff.second = (Number) value.getField(i);
							}
							out.collect(pairBuff);
						}
					}
				}
			}).name("flatten1D");

		/* sort data */
		Tuple2 <DataSet <PairComparable>, DataSet <Tuple2 <Integer, Long>>> sortedData
			= SortUtilsNext.pSort(flatten);
		return Tuple4.of(sortedData.f0, sortedData.f1, cnt, missingCount);
	}

	public static DataSet <FeatureBinsCalculator> transformModelToFeatureBins(DataSet <Row> modelDataSet,
																			  BinDivideType binDivideType) {
		return modelDataSet
			.reduceGroup(
				new GroupReduceFunction <Row, FeatureBinsCalculator>() {
					private static final long serialVersionUID = -483197241292759310L;

					@Override
					public void reduce(Iterable <Row> values, Collector <FeatureBinsCalculator> out) {
						List <Row> list = new ArrayList <>();
						values.forEach(list::add);
						QuantileDiscretizerModelDataConverter model
							= new QuantileDiscretizerModelDataConverter().load(list);
						for (ContinuousRanges featureInterval : model.data.values()) {
							out.collect(FeatureBinsCalculatorTransformer
								.fromContinuousFeatureInterval(featureInterval, binDivideType));
						}
					}
				}
			);
	}

	public static DataSet <Row> transformFeatureBinsToModel(DataSet <FeatureBinsCalculator> featureBorderDataSet) {
		return featureBorderDataSet.mapPartition(new MapPartitionFunction <FeatureBinsCalculator, Row>() {
			private static final long serialVersionUID = 5693661987734996860L;

			@Override
			public void mapPartition(Iterable <FeatureBinsCalculator> values, Collector <Row> out) throws Exception {
				transformFeatureBinsToModel(values, out);
			}
		}).setParallelism(1);
	}

	public static void transformFeatureBinsToModel(Iterable <FeatureBinsCalculator> values, Collector <Row> out) {
		List <String> selectedCols = new ArrayList <>();
		Map <String, ContinuousRanges> m = new HashMap <>();
		for (FeatureBinsCalculator featureBinsCalculator : values) {
			m.put(featureBinsCalculator.getFeatureName(),
				FeatureBinsCalculatorTransformer.toContinuousFeatureInterval(featureBinsCalculator));
			selectedCols.add(featureBinsCalculator.getFeatureName());
		}
		Params meta = new Params().set(QuantileDiscretizerTrainParams.SELECTED_COLS,
			selectedCols.toArray(new String[0]));
		QuantileDiscretizerModelDataConverter model = new QuantileDiscretizerModelDataConverter(m, meta);

		model.save(model, out);
	}

	@Override
	public QuantileDiscretizerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
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
		DataSet <Row> input = Preprocessing.select(in, quantileColNames).getDataSet();

		DataSet <Row> quantile = quantile(
			input, quantileNum,
			getParams().get(HasRoundMode.ROUND_MODE),
			getParams().get(Preprocessing.ZERO_AS_MISSING)
		);

		quantile = quantile.reduceGroup(
			new SerializeModel(
				getParams(),
				quantileColNames,
				TableUtil.findColTypesWithAssertAndHint(in.getSchema(), quantileColNames)
			)
		);

		/* set output */
		setOutput(quantile, new QuantileDiscretizerModelDataConverter().getModelSchema());

		return this;
	}

	@Override
	public QuantileDiscretizerModelInfoBatchOp getModelInfoBatchOp() {
		return new QuantileDiscretizerModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

	public static class MultiQuantile
		extends RichMapPartitionFunction <PairComparable, Tuple2 <Integer, Number>> {
		private static final long serialVersionUID = -467677491431226184L;
		protected int[] quantileNum;
		private List <Tuple2 <Integer, Long>> counts;
		private List <Tuple2 <Integer, Long>> missingCounts;
		private long totalCnt = 0;
		private HasRoundMode.RoundMode roundType;
		private int taskId;

		public MultiQuantile(int[] quantileNum, HasRoundMode.RoundMode roundType) {
			this.quantileNum = quantileNum;
			this.roundType = roundType;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			LOG.info("{} open count.", getRuntimeContext().getTaskName());
			this.counts = getRuntimeContext().getBroadcastVariableWithInitializer(
				"counts",
				new BroadcastVariableInitializer <Tuple2 <Integer, Long>, List <Tuple2 <Integer, Long>>>() {
					@Override
					public List <Tuple2 <Integer, Long>> initializeBroadcastVariable(
						Iterable <Tuple2 <Integer, Long>> data) {
						ArrayList <Tuple2 <Integer, Long>> sortedData = new ArrayList <>();
						for (Tuple2 <Integer, Long> datum : data) {
							sortedData.add(datum);
						}

						sortedData.sort(Comparator.comparing(o -> o.f0));

						return sortedData;
					}
				});

			LOG.info("{} open totalCnt.", getRuntimeContext().getTaskName());
			this.totalCnt = getRuntimeContext().getBroadcastVariableWithInitializer("totalCnt",
				new BroadcastVariableInitializer <Long, Long>() {
					@Override
					public Long initializeBroadcastVariable(Iterable <Long> data) {
						return data.iterator().next();
					}
				});

			LOG.info("{} open missingCounts.", getRuntimeContext().getTaskName());
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

			taskId = getRuntimeContext().getIndexOfThisSubtask();
			LOG.info("{} open.", getRuntimeContext().getTaskName());
		}

		@Override
		public void close() throws Exception {
			super.close();
			LOG.info("{} close.", getRuntimeContext().getTaskName());
		}

		@Override
		public void mapPartition(Iterable <PairComparable> values, Collector <Tuple2 <Integer, Number>> out)
			throws Exception {

			LOG.info("{} mapPartition start.", getRuntimeContext().getTaskName());

			long start = 0;
			long end;

			int curListIndex = -1;
			int size = counts.size();

			for (int i = 0; i < size; ++i) {
				int curId = counts.get(i).f0;

				if (curId == taskId) {
					curListIndex = i;
					break;
				}

				if (curId > taskId) {
					throw new RuntimeException("Error curId: " + curId
						+ ". id: " + taskId);
				}

				start += counts.get(i).f1;
			}

			end = start + counts.get(curListIndex).f1;

			ArrayList <PairComparable> allRows = new ArrayList <>((int) (end - start));

			for (PairComparable value : values) {
				allRows.add(value);
			}

			if (allRows.isEmpty()) {
				return;
			}

			if (allRows.size() != end - start) {
				throw new Exception("Error start end."
					+ " start: " + start
					+ ". end: " + end
					+ ". size: " + allRows.size());
			}

			LOG.info("taskId: {}, size: {}", getRuntimeContext().getIndexOfThisSubtask(), allRows.size());

			allRows.sort(Comparator.naturalOrder());

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

				if (totalCnt - missingCounts.get(fIdx).f1 == 0) {
					localStart += subEnd - subStart;
					continue;
				}

				QIndex qIndex = new QIndex(
					totalCnt - missingCounts.get(fIdx).f1, quantileNum[fIdx], roundType);

				for (int j = 1; j < quantileNum[fIdx]; ++j) {
					long index = qIndex.genIndex(j);

					if (index >= subStart && index < subEnd) {
						PairComparable pairComparable = allRows.get(
							(int) (index + localStart - subStart));
						out.collect(Tuple2.of(pairComparable.first, pairComparable.second));
					}
				}

				localStart += subEnd - subStart;
			}

			LOG.info("{} mapPartition end.", getRuntimeContext().getTaskName());
		}
	}

	public static class ReduceQuantile extends RichGroupReduceFunction <Tuple2 <Integer, Number>, Row> {
		private static final long serialVersionUID = 9176005213564219097L;

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
		public void reduce(Iterable <Tuple2 <Integer, Number>> values, Collector <Row> out) throws Exception {
			TreeSet <Number> set = new TreeSet <>(new Comparator <Number>() {
				@Override
				public int compare(Number o1, Number o2) {
					return SortUtils.OBJECT_COMPARATOR.compare(o1, o2);
				}
			});

			int id = -1;
			for (Tuple2 <Integer, Number> val : values) {
				id = val.f0;
				set.add(val.f1);
			}

			out.collect(Row.of(id, set.toArray(new Number[0])));
		}
	}

	public static class SerializeModel extends RichGroupReduceFunction <Row, Row> {
		private static final long serialVersionUID = 7835845627485620888L;
		protected String[] colNames;
		protected TypeInformation <?>[] colTypes;
		private Params meta;

		public SerializeModel(Params meta, String[] colNames, TypeInformation <?>[] colTypes) {
			this.meta = meta;
			this.colNames = colNames;
			this.colTypes = colTypes;
		}

		@Override
		public void reduce(Iterable <Row> values, Collector <Row> out) throws Exception {
			Map <String, ContinuousRanges> m = new HashMap <>();
			for (Row val : values) {
				int index = (int) val.getField(0);
				Number[] splits = (Number[]) val.getField(1);
				m.put(
					colNames[index],
					QuantileDiscretizerModelDataConverter.arraySplit2ContinuousRanges(
						colNames[index],
						colTypes[index],
						splits,
						meta.get(QuantileDiscretizerTrainParams.LEFT_OPEN)
					)
				);
			}

			for (int i = 0; i < colNames.length; ++i) {
				if (m.containsKey(colNames[i])) {
					continue;
				}

				m.put(
					colNames[i],
					QuantileDiscretizerModelDataConverter.arraySplit2ContinuousRanges(
						colNames[i],
						colTypes[i],
						null,
						meta.get(QuantileDiscretizerTrainParams.LEFT_OPEN)
					)
				);
			}

			QuantileDiscretizerModelDataConverter model = new QuantileDiscretizerModelDataConverter(m, meta);

			model.save(model, out);
		}
	}

	public static class QIndex {
		private double totalCount;
		private double q1;
		private HasRoundMode.RoundMode roundMode;

		public QIndex(double totalCount, int quantileNum, HasRoundMode.RoundMode type) {
			this.totalCount = totalCount;
			this.q1 = 1.0 / (double) quantileNum;
			this.roundMode = type;
		}

		public long genIndex(int k) {
			return roundMode.calc(this.q1 * (this.totalCount - 1.0) * (double) k);
		}
	}

}
