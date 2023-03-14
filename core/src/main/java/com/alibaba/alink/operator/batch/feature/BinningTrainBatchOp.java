package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.SelectedColsWithFirstInputSpec;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.common.viz.AlinkViz;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.viz.VizDataWriterInterface;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.DataSetUtil;
import com.alibaba.alink.operator.common.dataproc.StringIndexerUtil;
import com.alibaba.alink.operator.common.feature.BinningModelDataConverter;
import com.alibaba.alink.operator.common.feature.BinningModelInfo;
import com.alibaba.alink.operator.common.feature.BinningModelMapper;
import com.alibaba.alink.operator.common.feature.OneHotModelDataConverter;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.operator.common.feature.binning.BinDivideType;
import com.alibaba.alink.operator.common.feature.binning.BinTypes;
import com.alibaba.alink.operator.common.feature.binning.BinningModelInfoBatchOp;
import com.alibaba.alink.operator.common.feature.binning.Bins;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import com.alibaba.alink.operator.common.similarity.SerializableComparator;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.operator.common.statistics.statistics.IntervalCalculator;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.feature.HasDropLast;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoe;
import com.alibaba.alink.params.feature.QuantileDiscretizerTrainParams;
import com.alibaba.alink.params.finance.BinningTrainParams;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.TransformerBase;
import com.alibaba.alink.pipeline.feature.OneHotEncoderModel;
import com.alibaba.alink.pipeline.feature.QuantileDiscretizerModel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(value = PortType.MODEL, desc = PortDesc.OUTPUT_RESULT))
@SelectedColsWithFirstInputSpec
@NameCn("分箱训练")
@NameEn("binning trainer")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.Binning")
public final class BinningTrainBatchOp extends BatchOperator <BinningTrainBatchOp>
	implements BinningTrainParams <BinningTrainBatchOp>, AlinkViz <BinningTrainBatchOp>,
	WithModelInfoBatchOp <BinningModelInfo, BinningTrainBatchOp, BinningModelInfoBatchOp> {

	private static final long serialVersionUID = 2424584385121349839L;
	private static String FEATURE_DELIMITER = ",";
	private static String KEY_VALUE_DELIMITER = ":";
	private static int BUCKET_NUMBER = 10000;
	private static long DATA_ID_MAP = 1L;

	public BinningTrainBatchOp() {}

	public BinningTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public BinningTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		List <TransformerBase> transformers = new ArrayList <>();
		DataSet <FeatureBinsCalculator> featureBorderDataSet = null;

		if (this.getFromUserDefined()) {
			Preconditions.checkNotNull(this.getUserDefinedBin(), "User defined bin is empty!");
			//numeric, discrete
			Tuple5 <List <FeatureBinsCalculator>, HashSet <String>, List <FeatureBinsCalculator>, HashSet <String>,
				String[]> featureBorders = parseUserDefined(this.getParams());
			//check whether the bindividetype and leftOpen is the same
			Tuple2 <BinDivideType, Boolean> binDivideTypeLeftOpen = parseUserDefinedNumeric(featureBorders.f0);
			//update params
			this.setSelectedCols(featureBorders.f4)
				.setBinningMethod(BinningMethod.valueOf(binDivideTypeLeftOpen.f0.name()))
				.setLeftOpen(binDivideTypeLeftOpen.f1);

			//addOneHotModel
			if (featureBorders.f2.size() > 0) {
				Tuple2 <DataSet <FeatureBinsCalculator>, OneHotEncoderModel> tuple = discreteFromUserDefined(
					featureBorders.f2,
					getMLEnvironmentId(), featureBorders.f3.toArray(new String[0]));
				featureBorderDataSet = tuple.f0;
				transformers.add(tuple.f1);
			}
			//addQuantileModel
			if (featureBorders.f0.size() > 0) {
				Tuple2 <DataSet <FeatureBinsCalculator>, QuantileDiscretizerModel> tuple = numericFromUserDefined(
					featureBorders.f0,
					getMLEnvironmentId(), featureBorders.f1.toArray(new String[0]));
				transformers.add(tuple.f1);
				featureBorderDataSet = null == featureBorderDataSet ? tuple.f0 : featureBorderDataSet.union(tuple.f0);
			}
		} else {
			//numeric, discrete
			Tuple2 <List <String>, List <String>> t = BinningModelMapper.distinguishNumericDiscrete(
				this.getSelectedCols(),
				in.getSchema());
			//add onehot model
			if (t.f1.size() > 0) {
				Tuple2 <DataSet <FeatureBinsCalculator>, OneHotEncoderModel> tuple = discreteFromTrain(in,
					t.f1.toArray(new String[0]), getParams());
				featureBorderDataSet = tuple.f0;
				transformers.add(tuple.f1);
			}
			//add quantile model
			if (t.f0.size() > 0) {
				Tuple2 <DataSet <FeatureBinsCalculator>, QuantileDiscretizerModel> tuple = numericFromTrain(in,
					t.f0.toArray(new String[0]), getParams());
				transformers.add(tuple.f1);
				featureBorderDataSet = null == featureBorderDataSet ? tuple.f0 : featureBorderDataSet.union(tuple.f0);
			}
		}

		Preconditions.checkNotNull(featureBorderDataSet, "No binning is generated, please check input!");

		PipelineModel pipelineModel = new PipelineModel(transformers.toArray(new TransformerBase[0]));
		BatchOperator index = pipelineModel.transform(in);
		featureBorderDataSet = featureBinsStatistics(featureBorderDataSet, index, getParams());

		DataSet <Row> featureBorderModel = featureBorderDataSet
			.mapPartition(new SerializeFeatureBinsModel())
			.name("SerializeModel");

		VizDataWriterInterface writer = this.getVizDataWriter();
		if (writer != null) {
			writeVizData(featureBorderDataSet, writer, getSelectedCols());
			writePreciseVizData(in, featureBorderDataSet, getParams(), writer);
		}

		this.setOutput(featureBorderModel, new BinningModelDataConverter().getModelSchema());
		return this;
	}

	private static class FeatureBinsKey implements KeySelector <FeatureBinsCalculator, String> {
		private static final long serialVersionUID = 4363650897636276540L;

		@Override
		public String getKey(FeatureBinsCalculator calculator) {
			return calculator.getFeatureName();
		}
	}

	static DataSet <FeatureBinsCalculator> featureBinsStatistics(DataSet <FeatureBinsCalculator> featureBorderDataSet,
																 BatchOperator index, Params param) {
		if (null != param.get(BinningTrainParams.LABEL_COL)) {
			Preconditions.checkArgument(param.contains(BinningTrainParams.POS_LABEL_VAL_STR),
				"PositiveValue is not set!");
			return setFeatureBinsTotalAndWoe(featureBorderDataSet, index, param);
		} else {
			return setFeatureBinsTotal(featureBorderDataSet, index, param.get(BinningTrainParams.SELECTED_COLS));
		}
	}

	static class SerializeFeatureBinsModel extends RichMapPartitionFunction <FeatureBinsCalculator, Row> {
		private static final long serialVersionUID = 2703991631667194660L;

		@Override
		public void mapPartition(Iterable <FeatureBinsCalculator> iterable, Collector <Row> collector) {
			new BinningModelDataConverter().save(iterable, collector);
		}
	}

	public static Tuple2 <DataSet <FeatureBinsCalculator>, QuantileDiscretizerModel> numericFromTrain(BatchOperator
																										   in,
																									   String[]
																										   numericCols,
																									   Params params) {
		BinningMethod binningMethod = params.get(BinningTrainParams.BINNIG_METHOD);
		switch (binningMethod) {
			case QUANTILE: {
				return quantileTrain(in, numericCols, params);
			}
			case BUCKET: {
				return bucketTrain(in, numericCols, params);
			}
			default: {
				throw new IllegalArgumentException("Not support binningMethod: " + binningMethod);
			}
		}
	}

	public static Tuple2 <DataSet <FeatureBinsCalculator>, OneHotEncoderModel> discreteFromTrain(BatchOperator in,
																								  String[]
																									  discreteCols,
																								  Params params) {
		Integer[] discreteThreshold = getValueArray(params, BinningTrainParams.DISCRETE_THRESHOLDS,
			BinningTrainParams.DISCRETE_THRESHOLDS_ARRAY, BinningTrainParams.DISCRETE_THRESHOLDS_MAP, discreteCols);

		OneHotTrainBatchOp oneHot = new OneHotTrainBatchOp()
			.setSelectedCols(discreteCols)
			.setDiscreteThresholdsArray(discreteThreshold)
			.linkFrom(in);

		DataSet <FeatureBinsCalculator> featureBinsCalculator = OneHotTrainBatchOp
			.transformModelToFeatureBins(oneHot.getDataSet());
		OneHotEncoderModel model = setOneHotModelData(oneHot, discreteCols, params.get(ML_ENVIRONMENT_ID));
		return Tuple2.of(featureBinsCalculator, model);
	}

	private static Tuple2 <DataSet <FeatureBinsCalculator>, QuantileDiscretizerModel> bucketTrain(BatchOperator in,
																								  String[] numericCols,
																								  Params params) {
		EqualWidthDiscretizerTrainBatchOp quantile = new EqualWidthDiscretizerTrainBatchOp(
			generateNumericTrainParams(params, numericCols))
			.linkFrom(in);

		DataSet <FeatureBinsCalculator> featureBinsCalculator = QuantileDiscretizerTrainBatchOp
			.transformModelToFeatureBins(quantile.getDataSet(), BinDivideType.BUCKET);

		QuantileDiscretizerModel model = setQuantileDiscretizerModelData(
			quantile,
			numericCols,
			params.get(ML_ENVIRONMENT_ID));
		return Tuple2.of(featureBinsCalculator, model);
	}

	private static Tuple2 <DataSet <FeatureBinsCalculator>, QuantileDiscretizerModel> quantileTrain(BatchOperator in,
																									String[]
																										numericCols,
																									Params params) {
		QuantileDiscretizerTrainBatchOp quantile = new QuantileDiscretizerTrainBatchOp(
			generateNumericTrainParams(params, numericCols))
			.linkFrom(in);

		DataSet <FeatureBinsCalculator> featureBinsCalculator = QuantileDiscretizerTrainBatchOp
			.transformModelToFeatureBins(quantile.getDataSet(), BinDivideType.QUANTILE);

		QuantileDiscretizerModel model = setQuantileDiscretizerModelData(
			quantile,
			numericCols,
			params.get(ML_ENVIRONMENT_ID));
		return Tuple2.of(featureBinsCalculator, model);
	}

	private static Tuple2 <DataSet <FeatureBinsCalculator>, OneHotEncoderModel> discreteFromUserDefined(
		List <FeatureBinsCalculator> featureBinsCalculators,
		long environmentId,
		String[] discreteCols) {
		DataSet <FeatureBinsCalculator> featureBorderDataSet = MLEnvironmentFactory
			.get(environmentId)
			.getExecutionEnvironment()
			.fromCollection(featureBinsCalculators)
			.name("DiscreteFromUserDefined");

		OneHotEncoderModel model = setOneHotModelData(
			BatchOperator.fromTable(
				DataSetConversionUtil
					.toTable(environmentId, OneHotTrainBatchOp.transformFeatureBinsToModel(featureBorderDataSet),
						new OneHotModelDataConverter().getModelSchema())
			)
			, discreteCols, environmentId);

		return Tuple2.of(featureBorderDataSet, model);
	}

	private static Tuple2 <DataSet <FeatureBinsCalculator>, QuantileDiscretizerModel> numericFromUserDefined(
		List <FeatureBinsCalculator> featureBinsCalculators,
		long environmentId,
		String[] numericCols) {
		DataSet <FeatureBinsCalculator> featureBorderDataSet = MLEnvironmentFactory
			.get(environmentId)
			.getExecutionEnvironment()
			.fromCollection(featureBinsCalculators)
			.name("NumericFromUserDefined");

		QuantileDiscretizerModel model = setQuantileDiscretizerModelData(
			BatchOperator.fromTable(
				DataSetConversionUtil.toTable(environmentId,
					QuantileDiscretizerTrainBatchOp.transformFeatureBinsToModel(featureBorderDataSet),
					new QuantileDiscretizerModelDataConverter().getModelSchema())
			)
			, numericCols,
			environmentId);

		return Tuple2.of(featureBorderDataSet, model);
	}

	private static Tuple5 <List <FeatureBinsCalculator>, HashSet <String>, List <FeatureBinsCalculator>, HashSet
		<String>, String[]> parseUserDefined(Params params) {
		HashSet <String> userDefinedNumeric = new HashSet <>();
		HashSet <String> userDefinedDiscrete = new HashSet <>();
		String[] selectedCols = params.get(BinningTrainBatchOp.SELECTED_COLS);
		Tuple2 <List <FeatureBinsCalculator>, List <FeatureBinsCalculator>> numericDiscrete = BinningModelMapper
			.distinguishNumericDiscrete(
				FeatureBinsUtil.deSerialize(params.get(BinningTrainBatchOp.USER_DEFINED_BIN)),
				selectedCols,
				userDefinedNumeric,
				userDefinedDiscrete);

		String[] newSelectedCols = new String[userDefinedDiscrete.size() + userDefinedNumeric.size()];
		int c = 0;
		for (String s : selectedCols) {
			if (userDefinedDiscrete.contains(s) || userDefinedNumeric.contains(s)) {
				newSelectedCols[c++] = s;
			}
		}
		return Tuple5.of(numericDiscrete.f0, userDefinedNumeric, numericDiscrete.f1, userDefinedDiscrete,
			newSelectedCols);
	}

	private static Tuple2 <BinDivideType, Boolean> parseUserDefinedNumeric(List <FeatureBinsCalculator> list) {
		BinDivideType binDivideType = null;
		Boolean leftOpen = null;
		for (FeatureBinsCalculator calculator : list) {
			Preconditions.checkArgument(calculator.isNumeric(), "parseUserDefinedNumeric only supports numeric bins!");
			if (null == binDivideType) {
				binDivideType = calculator.getBinDivideType();
				leftOpen = calculator.getLeftOpen();
			} else {
				Preconditions.checkArgument(binDivideType.equals(calculator.getBinDivideType()),
					"Features have different BinDivideType!");
				Preconditions.checkArgument(leftOpen.equals(calculator.getLeftOpen()),
					"Features have different leftOpen params!");
			}
		}
		return Tuple2.of(binDivideType, leftOpen);
	}

	public static DataSet <FeatureBinsCalculator> setFeatureBinsTotalAndWoe(
		DataSet <FeatureBinsCalculator> featureBorderDataSet,
		BatchOperator index,
		Params params) {
		Preconditions.checkArgument(TableUtil.findColIndex(params.get(BinningTrainBatchOp.SELECTED_COLS),
			params.get(BinningTrainBatchOp.LABEL_COL)) < 0,
			"labelCol is included in selectedCols");
		WoeTrainBatchOp op = new WoeTrainBatchOp(params).linkFrom(index);
		return WoeTrainBatchOp.setFeatureBinsWoe(featureBorderDataSet, op.getDataSet());
	}

	static DataSet <FeatureBinsCalculator> setFeatureBinsTotal(DataSet <FeatureBinsCalculator> featureBorderDataSet,
															   BatchOperator index,
															   String[] selectedCols) {
		DataSet <Tuple3 <Integer, String, Long>> tokenCounts = StringIndexerUtil.countTokens(
			index.select(selectedCols).getDataSet(), true);
		DataSet <Tuple2 <String, FeatureBinsCalculator>> borderWithName = featureBorderDataSet.map(
			new MapFunction <FeatureBinsCalculator, Tuple2 <String, FeatureBinsCalculator>>() {
				private static final long serialVersionUID = 5564348245552253677L;

				@Override
				public Tuple2 <String, FeatureBinsCalculator> map(FeatureBinsCalculator value) {
					return Tuple2.of(value.getFeatureName(), value);
				}
			});

		DataSet <Tuple2 <String, Map <Long, Long>>> featureCounts = tokenCounts
			.groupBy(0)
			.reduceGroup(new GroupReduceFunction <Tuple3 <Integer, String, Long>, Tuple2 <String, Map <Long, Long>>>
				() {
				private static final long serialVersionUID = -3648772506771320958L;

				@Override
				public void reduce(Iterable <Tuple3 <Integer, String, Long>> values,
								   Collector <Tuple2 <String, Map <Long, Long>>> out) {
					String featureName = null;
					Map <Long, Long> map = new HashMap <>();
					for (Tuple3 <Integer, String, Long> t : values) {
						featureName = selectedCols[t.f0];
						map.put(Long.valueOf(t.f1), t.f2);
					}
					if (null != featureName) {
						out.collect(Tuple2.of(featureName, map));
					}
				}
			}).name("GetBinTotalMap");

		return borderWithName
			.join(featureCounts)
			.where(0)
			.equalTo(0)
			.with(
				new JoinFunction <Tuple2 <String, FeatureBinsCalculator>, Tuple2 <String, Map <Long, Long>>,
					FeatureBinsCalculator>() {
					private static final long serialVersionUID = -7326232704777819964L;

					@Override
					public FeatureBinsCalculator join(Tuple2 <String, FeatureBinsCalculator> first,
													  Tuple2 <String, Map <Long, Long>> second) {
						FeatureBinsCalculator border = first.f1;
						border.setTotal(second.f1);
						return border;
					}
				}).name("SetBinTotal");
	}

	private static Params generateNumericTrainParams(Params params, String[] numericCols) {
		Integer[] discreteThreshold = getValueArray(params, BinningTrainParams.NUM_BUCKETS,
			BinningTrainParams.NUM_BUCKETS_ARRAY, BinningTrainParams.NUM_BUCKETS_MAP, numericCols);
		return new Params()
			.set(QuantileDiscretizerTrainParams.SELECTED_COLS, numericCols)
			.set(QuantileDiscretizerTrainParams.NUM_BUCKETS_ARRAY, discreteThreshold)
			.set(QuantileDiscretizerTrainParams.LEFT_OPEN, params.get(BinningTrainParams.LEFT_OPEN));
	}

	private static OneHotEncoderModel setOneHotModelData(BatchOperator <?> modelData,
														 String[] selectedCols,
														 long environmentId) {
		OneHotEncoderModel oneHotEncode = new OneHotEncoderModel(
			encodeIndexForWoeTrainParams(selectedCols, environmentId));
		oneHotEncode.setModelData(modelData);
		return oneHotEncode;
	}

	static QuantileDiscretizerModel setQuantileDiscretizerModelData(
		BatchOperator <?> modelData,
		String[] selectedCols,
		long environmentId) {
		QuantileDiscretizerModel quantileDiscretizerModel = new QuantileDiscretizerModel(
			encodeIndexForWoeTrainParams(selectedCols, environmentId));
		quantileDiscretizerModel.setModelData(modelData);
		return quantileDiscretizerModel;
	}

	public static Params encodeIndexForWoeTrainParams(String[] selectedCols, long environmentId) {
		return new Params()
			.set(HasMLEnvironmentId.ML_ENVIRONMENT_ID, environmentId)
			.set(HasSelectedCols.SELECTED_COLS, selectedCols)
			.set(HasHandleInvalid.HANDLE_INVALID, HasHandleInvalid.HandleInvalid.KEEP)
			.set(HasDropLast.DROP_LAST, false)
			.set(HasEncodeWithoutWoe.ENCODE, HasEncodeWithoutWoe.Encode.INDEX);
	}

	//write quick binning viz data
	private static void writePreciseVizData(BatchOperator in,
											DataSet <FeatureBinsCalculator> originFeatureBins,
											Params params,
											VizDataWriterInterface writer) {
		//numeric, discrete
		Tuple2 <List <String>, List <String>> t = BinningModelMapper.distinguishNumericDiscrete(
			params.get(BinningTrainParams.SELECTED_COLS),
			in.getSchema());
		//add quantile model
		if (t.f0.size() > 0) {
			params.set(BinningTrainParams.SELECTED_COLS, t.f0.toArray(new String[0]));
			DataSet <TableSummary> summary = StatisticsHelper.summary(in, t.f0.toArray(new String[0]));
			DataSet <FeatureBinsCalculator> preciseFeatureBins = originFeatureBins.flatMap(
				new RichFlatMapFunction <FeatureBinsCalculator, FeatureBinsCalculator>() {
					private static final long serialVersionUID = -1054858627945256161L;

					@Override
					public void flatMap(FeatureBinsCalculator value, Collector <FeatureBinsCalculator> out) {
						BinTypes.ColType colType = value.getColType();
						if (!colType.isNumeric) {
							return;
						}
						Number[] numbers = value.getSplitsArray();
						boolean isFloat = colType.equals(BinTypes.ColType.FLOAT);
						for (Number number2 : numbers) {
							isFloat |= (number2 instanceof Double || number2 instanceof Float);
						}
						TreeSet <Number> set = generateCloseBucket(numbers, isFloat);
						String fatureName = value.getFeatureName();

						TableSummary summary = (TableSummary) getRuntimeContext().getBroadcastVariable("summary").get(
							0);
						set.addAll(
							generateGivenNumberBucket(summary.minDouble(fatureName), summary.maxDouble(fatureName), isFloat));

						out.collect(FeatureBinsCalculator.createNumericCalculator(value.getBinDivideType(),
							value.getFeatureName(),
							FeatureBinsUtil.getFlinkType(value.getFeatureType()),
							set.toArray(new Number[0]),
							value.getLeftOpen()));
					}
				}).withBroadcastSet(summary, "summary");

			QuantileDiscretizerModel model = setQuantileDiscretizerModelData(
				BatchOperator.fromTable(
					DataSetConversionUtil.toTable(in.getMLEnvironmentId(),
						QuantileDiscretizerTrainBatchOp.transformFeatureBinsToModel(preciseFeatureBins),
						new QuantileDiscretizerModelDataConverter().getModelSchema())
				),
				params.get(BinningTrainParams.SELECTED_COLS),
				in.getMLEnvironmentId());
			BatchOperator preciseCut = model.transform(in);
			preciseFeatureBins = featureBinsStatistics(preciseFeatureBins, preciseCut, params);
			writePreciseVizData(preciseFeatureBins, originFeatureBins, params.get(BinningTrainParams.SELECTED_COLS),
				writer);
		}
	}

	static List <Number> generateGivenNumberBucket(double minDecimal, double maxDecimal, boolean isFloat) {
		List <Number> list = new ArrayList <>();
		if (!isFloat) {
			long min = (long) minDecimal;
			long max = (long) maxDecimal;
			IntervalCalculator intervalCalculator = IntervalCalculator.create(new long[] {min - 1, max + 1},
				BUCKET_NUMBER);
			long start = intervalCalculator.getLeftBound().longValue();
			long step = intervalCalculator.getStep().longValue();
			for (int i = 0; i < intervalCalculator.getCount().length; i++) {
				list.add(start);
				start += step;
			}
		} else {
			IntervalCalculator intervalCalculator = IntervalCalculator.create(
				new double[] {minDecimal - 0.1, maxDecimal + 0.1}, BUCKET_NUMBER);
			double start = intervalCalculator.getLeftBound().doubleValue();
			double step = intervalCalculator.getStep().doubleValue();
			for (int i = 0; i < intervalCalculator.n; i++) {
				list.add(start);
				start += step;
			}
		}
		return list;
	}

	static TreeSet <Number> generateCloseBucket(Number[] numbers, boolean isFloat) {
		TreeSet <Number> set = new TreeSet <>();
		if (!isFloat) {
			for (Number number1 : numbers) {
				set.add(number1.longValue());
				boolean negative = (number1.longValue() < 0);
				long number = Math.abs(number1.longValue());
				long deno = 1;
				while (deno <= number * 10) {
					long tmp = number / deno * deno;
					set.add(negative ? -tmp : tmp);
					set.add(negative ? -(tmp + deno) : tmp + deno);
					deno *= 10;
				}
			}
		} else {
			for (Number number1 : numbers) {
				set.add(number1.doubleValue());
				boolean negative = (number1.doubleValue() < 0);
				double number = Math.abs(number1.doubleValue());
				if (number < 1) {
					int deno = 1;
					double target = number * deno;
					while (Double.compare(target, Math.floor(target)) != 0) {
						double tmp = Math.floor(target);
						set.add(negative ? -tmp / deno : tmp / deno);
						set.add(negative ? -(tmp + 1) / deno : (tmp + 1) / deno);
						deno *= 10;
						target = number * deno;
					}
				} else {
					long numberN = Math.abs(number1.longValue());
					long deno = 1;
					while (deno <= number * 10) {
						long tmp = numberN / deno * deno;
						set.add(negative ? -(double) tmp : (double) tmp);
						set.add(negative ? -(double) (tmp + deno) : (double) (tmp + deno));
						deno *= 10;
					}


				}
			}
		}
		return set;
	}

	private static void writePreciseVizData(DataSet <FeatureBinsCalculator> featureBorderDataSet,
											DataSet <FeatureBinsCalculator> originFeatureBins,
											String[] selecteCols,
											VizDataWriterInterface writer) {
		Map <String, Long> keyId = new HashMap <>();
		//write map: column name : column id
		long start = DATA_ID_MAP + 1L;
		for (String col : selecteCols) {
			keyId.put(col, start++);
		}
		//System.out.println(JsonConverter.toJson(keyId));
		writer.writeBatchData(DATA_ID_MAP, JsonConverter.toJson(keyId), System.currentTimeMillis());

		DataSet <Row> dummy = featureBorderDataSet.join(originFeatureBins)
			.where(new FeatureBinsKey())
			.equalTo(new FeatureBinsKey())
			.with(new JoinFunction <FeatureBinsCalculator, FeatureBinsCalculator, Row>() {
				private static final long serialVersionUID = 4855360130213173442L;

				@Override
				public Row join(FeatureBinsCalculator first, FeatureBinsCalculator second) throws Exception {
					Preconditions.checkArgument(first.isNumeric(), "Precise only support numeric bins!");
					first.calcStatistics();
					List <Number> originBins = Arrays.asList(second.getSplitsArray());
					Number[] cutsArray = first.getSplitsArray();
					Bins bins = first.getBin();
					List <IntervalStatistics> map = new ArrayList <>();
					List <Number> cutsMap = new ArrayList <>();
					Integer positive = null;
					int total = 0;
					int pre = -1;
					for (int i = 0; i < bins.normBins.size(); i++) {
						Bins.BaseBin baseBin = bins.normBins.get(i);
						//keep the original cutsArray, only keep two cuts whose total are the same
						if (baseBin.getTotal() > 0 || pre != 0 || originBins.contains(cutsArray[i - 1])) {
							pre = baseBin.getTotal().intValue();
							total += pre;
							if (first.getPositiveTotal() != null) {
								positive = ((null == positive) ? baseBin.getPositive().intValue()
									: positive + baseBin.getPositive().intValue());
							}
							map.add(new IntervalStatistics(total, positive));
							if (i > 0) {
								cutsMap.add(cutsArray[i - 1]);
							}
						}
					}
					int index = TableUtil.findColIndexWithAssert(selecteCols, first.getFeatureName());
					Params params = new Params().set("Interval", cutsMap);
					params.set("Statistics", map);
					//System.out.println((DATA_ID_MAP + 1 + index) + ":" + params.toJson());
					writer.writeBatchData((DATA_ID_MAP + 1 + index), params.toJson(), System.currentTimeMillis());
					return new Row(1);
				}
			});
		DataSetUtil.linkDummySink(dummy);
	}

	private static void writeVizData(DataSet <FeatureBinsCalculator> featureBorderDataSet,
									 VizDataWriterInterface writer,
									 String[] selectedCols) {
		DataSet <Row> dummy = featureBorderDataSet.mapPartition(
			new MapPartitionFunction <FeatureBinsCalculator, Row>() {
				private static final long serialVersionUID = 1298967177624132843L;

				@Override
				public void mapPartition(Iterable <FeatureBinsCalculator> values, Collector <Row> out) {
					List <FeatureBinsCalculator> list = new ArrayList <>();
					values.forEach(list::add);
					list.sort(new SerializableComparator <FeatureBinsCalculator>() {
						private static final long serialVersionUID = -6390285370495541755L;

						@Override
						public int compare(FeatureBinsCalculator o1, FeatureBinsCalculator o2) {
							return TableUtil.findColIndex(selectedCols, o1.getFeatureName()) < TableUtil.findColIndex(
								selectedCols, o2.getFeatureName()) ? -1 : 1;
						}
					});
					//System.out.println(0L + ":" + FeatureBinsUtil.serialize(list.toArray(new
					// FeatureBinsCalculator[0])));
					writer.writeBatchData(0L, FeatureBinsUtil.serialize(list.toArray(new FeatureBinsCalculator[0])),
						System.currentTimeMillis());
				}
			}).setParallelism(1).name("WriteFeatureBinsViz");
		DataSetUtil.linkDummySink(dummy);
	}

	static Integer[] getValueArray(Params params, ParamInfo <Integer> single,
								   ParamInfo <Integer[]> array,
								   ParamInfo <String> map,
								   String[] selectedCols) {
		Preconditions.checkArgument(!(params.contains(single) && (params.contains(array))),
			"It can not set " + single.getName() + " " + array.getName() + " at the same time!");
		Preconditions.checkArgument(!(params.contains(array) && (params.contains(map))),
			"It can not set " + map.getName() + " " + array.getName() + " at the same time!");

		Integer[] values;
		if (params.contains(array)) {
			values = params.get(array);
			Preconditions.checkArgument(values.length == selectedCols.length,
				"The length of %s must be equal to the length of train cols!", array.getName());
		} else {
			values = new Integer[selectedCols.length];
			Arrays.fill(values, params.get(single));
			if (params.contains(map)) {
				Map <String, Integer> keyValue = parseInputMap(params.get(map));
				for (Map.Entry <String, Integer> entry : keyValue.entrySet()) {
					int index = TableUtil.findColIndexWithAssertAndHint(selectedCols, entry.getKey());
					values[index] = entry.getValue();
				}
			}
		}
		return values;
	}

	static Map <String, Integer> parseInputMap(String str) {
		String[] cols = str.split(FEATURE_DELIMITER);
		Map <String, Integer> map = new HashMap <>();
		for (String s : cols) {
			String[] nameValue = s.split(KEY_VALUE_DELIMITER);
			Preconditions.checkArgument(nameValue.length == 2, "Input Map parse fail!");
			map.put(nameValue[0].trim(), Integer.valueOf(nameValue[1]));
		}
		return map;
	}

	static class IntervalStatistics implements Serializable {
		private static final long serialVersionUID = 7680582727625414983L;
		Integer total;
		Integer positive;

		public IntervalStatistics(Integer total, Integer positive) {
			this.total = total;
			this.positive = positive;
		}
	}

	@Override
	public BinningModelInfoBatchOp getModelInfoBatchOp() {
		return new BinningModelInfoBatchOp().linkFrom(this);
	}

}
