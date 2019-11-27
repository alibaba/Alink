package com.alibaba.alink.operator.common.tree;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.dataproc.NumericalTypeCastBatchOp;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasWeightCol;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.tree.HasMaxBins;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * Util class for data pre-processing in the random forest.
 */
public class Preprocessing {

	/**
	 * Sample count that be used in the quantile discretizer.
	 */
	public final static ParamInfo<Long> SAMPLE_COUNT_4_BIN = ParamInfoFactory
		.createParamInfo("sampleCount4Bin", Long.class)
		.setHasDefaultValue(500000L)
		.build();

	/**
	 * Distinct the label column and get the dataset of unique labels.
	 * @param input the training data.
	 * @return the dataset of unique label.
	 */
	public static DataSet <Object[]> distinctLabels(DataSet <Object> input) {
		return input
			.map(new MapFunction <Object, Tuple1 <Comparable>>() {
				@Override
				public Tuple1 <Comparable> map(Object value) throws Exception {
					return Tuple1.of((Comparable) value);
				}
			})
			.groupBy(0)
			.reduce(new ReduceFunction<Tuple1<Comparable>>() {
				@Override
				public Tuple1 <Comparable> reduce(Tuple1 <Comparable> in1, Tuple1 <Comparable> in2) throws Exception {
					return in1;
				}
			})
			.reduceGroup(new GroupReduceFunction<Tuple1<Comparable>, Object[]>(){
				@Override
				public void reduce(Iterable <Tuple1 <Comparable>> values, Collector <Object[]> out) throws Exception {
					out.collect(
						StreamSupport.stream(values.spliterator(), false)
							.map(x -> x.f0)
							.sorted(new SortUtils.ComparableComparator())
							.toArray(Object[]::new)
					);
				}
			});
	}

	/**
	 * Transform the label column as the
	 *
	 * @param input
	 * @param model
	 * @param colIdx
	 * @return
	 */
	public static DataSet <Row> findIndexOfLabel(DataSet <Row> input, DataSet <Object[]> model, final int colIdx) {
		return input
			.map(new RichMapFunction <Row, Row>() {
				Object[] model;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);

					model = getRuntimeContext().getBroadcastVariableWithInitializer("model",
						new BroadcastVariableInitializer <Object[], Object[]>() {
							@Override
							public Object[] initializeBroadcastVariable(Iterable <Object[]> data) {
								return data.iterator().next();
							}
						}
					);
				}

				@Override
				public Row map(Row value) throws Exception {
					value.setField(colIdx, findIdx(model, value.getField(colIdx)));
					return value;
				}
			})
			.withBroadcastSet(model, "model");
	}

	public static int findIdx(Object[] model, Object val) {
		int found = Arrays.binarySearch(model, val);

		if (found >= 0) {
			return found;
		} else {
			throw new RuntimeException("Can not find " + val);
		}
	}

	public static DataSet<Object[]> generateLabels(
		BatchOperator<?> input,
		Params params,
		boolean isRegression) {
		DataSet<Object[]> labels;
		if (!isRegression) {
			final String labelColName = params.get(HasLabelCol.LABEL_COL);
			final TypeInformation<?>[] types = input.getColTypes();

			DataSet<Row> labelDataSet = input.select(labelColName).getDataSet();

			labels = distinctLabels(labelDataSet
				.map(new MapFunction<Row, Object>() {
					@Override
					public Object map(Row value) throws Exception {
						return value.getField(0);
					}
				})
			);

		} else {
			labels = MLEnvironmentFactory.get(input.getMLEnvironmentId()).getExecutionEnvironment().fromElements(1)
				.mapPartition(new MapPartitionFunction<Integer, Object[]>() {
					@Override
					public void mapPartition(Iterable<Integer> values, Collector<Object[]> out) throws Exception {
						//pass
					}
				});
		}

		return labels;
	}

	public static BatchOperator<?> castLabel(
		BatchOperator<?> input, Params params, DataSet<Object[]> labels, boolean isRegression) {
		String[] inputColNames = input.getColNames();
		if (!isRegression) {
			final String labelColName = params.get(HasLabelCol.LABEL_COL);
			final TypeInformation<?>[] types = input.getColTypes();
			input = new DataSetWrapperBatchOp(
				findIndexOfLabel(
					input.getDataSet(), labels,
					TableUtil.findColIndex(inputColNames, labelColName)
				),
				input.getColNames(),
				IntStream.range(0, input.getColTypes().length)
					.mapToObj(x -> x == TableUtil.findColIndex(inputColNames, labelColName) ? Types.INT :
						types[x])
					.toArray(TypeInformation[]::new)

			).setMLEnvironmentId(input.getMLEnvironmentId());

		} else {
			if (params.contains(HasLabelCol.LABEL_COL)) {
				input = new NumericalTypeCastBatchOp()
					.setMLEnvironmentId(input.getMLEnvironmentId())
					.setSelectedCols(params.get(HasLabelCol.LABEL_COL))
					.setTargetType("DOUBLE")
					.linkFrom(input);
			}
		}

		return input;
	}

	public static BatchOperator<?> generateStringIndexerModel(BatchOperator<?> input, Params params) {
		String[] categoricalColNames = params.get(HasCategoricalCols.CATEGORICAL_COLS);
		BatchOperator<?> stringIndexerModel;
		if (categoricalColNames == null || categoricalColNames.length == 0) {
			MultiStringIndexerModelDataConverter emptyModel = new MultiStringIndexerModelDataConverter();

			stringIndexerModel = new DataSetWrapperBatchOp(
				MLEnvironmentFactory
					.get(input.getMLEnvironmentId())
					.getExecutionEnvironment()
					.fromElements(1)
					.mapPartition(new MapPartitionFunction<Integer, Row>() {
						@Override
						public void mapPartition(Iterable<Integer> values, Collector<Row> out) throws Exception {
							//pass
						}
					}),
				emptyModel.getModelSchema().getFieldNames(),
				emptyModel.getModelSchema().getFieldTypes()
			).setMLEnvironmentId(input.getMLEnvironmentId());
		} else {
			stringIndexerModel = new MultiStringIndexerTrainBatchOp()
				.setMLEnvironmentId(input.getMLEnvironmentId())
				.setSelectedCols(categoricalColNames)
				.linkFrom(input);
		}

		return stringIndexerModel;
	}

	public static BatchOperator<?> castCategoricalCols(
		BatchOperator<?> input,
		BatchOperator<?> stringIndexerModel,
		Params params) {
		String[] categoricalColNames = params.get(HasCategoricalCols.CATEGORICAL_COLS);
		if (categoricalColNames != null && categoricalColNames.length != 0) {
			input = new MultiStringIndexerPredictBatchOp()
				.setMLEnvironmentId(input.getMLEnvironmentId())
				.setHandleInvalid("skip")
				.setSelectedCols(categoricalColNames)
				.setReservedCols(input.getColNames())
				.linkFrom(stringIndexerModel, input)
				.select(input.getColNames());
			input = new NumericalTypeCastBatchOp()
				.setMLEnvironmentId(input.getMLEnvironmentId())
				.setSelectedCols(categoricalColNames)
				.setTargetType("INT")
				.linkFrom(input);
		}

		return input;
	}

	public static BatchOperator<?> castContinuousCols(
		BatchOperator<?> input,
		Params params) {
		String[] continuousColNames = ArrayUtils
			.removeElements(
				params.get(HasFeatureCols.FEATURE_COLS),
				params.get(HasCategoricalCols.CATEGORICAL_COLS)
			);

		if (continuousColNames != null && continuousColNames.length > 0) {
			input = new NumericalTypeCastBatchOp()
				.setMLEnvironmentId(input.getMLEnvironmentId())
				.setSelectedCols(continuousColNames)
				.setTargetType("DOUBLE")
				.linkFrom(input);
		}

		return input;
	}

	public static BatchOperator<?> castWeightCol(
		BatchOperator<?> input,
		Params params) {
		String weightCol = params.get(HasWeightColDefaultAsNull.WEIGHT_COL);
		if (weightCol == null) {
			return input;
		}

		return new NumericalTypeCastBatchOp()
			.setMLEnvironmentId(input.getMLEnvironmentId())
			.setSelectedCols(weightCol)
			.setTargetType("DOUBLE")
			.linkFrom(input);
	}

	public static BatchOperator<?> generateQuantileDiscretizerModel(
		BatchOperator<?> input,
		Params params) {
		String[] continuousColNames = ArrayUtils.removeElements(
			params.get(HasFeatureCols.FEATURE_COLS),
			params.get(HasCategoricalCols.CATEGORICAL_COLS)
		);
		BatchOperator<?> quantileDiscretizerModel;
		if (continuousColNames != null && continuousColNames.length > 0) {
			quantileDiscretizerModel = sample(input, params)
				.linkTo(new QuantileDiscretizerTrainBatchOp()
					.setMLEnvironmentId(input.getMLEnvironmentId())
					.setSelectedCols(continuousColNames)
					.setNumBuckets(params.get(HasMaxBins.MAX_BINS))
				);
		} else {
			QuantileDiscretizerModelDataConverter emptyModel = new QuantileDiscretizerModelDataConverter();

			quantileDiscretizerModel = new DataSetWrapperBatchOp(
				MLEnvironmentFactory
					.get(input.getMLEnvironmentId())
					.getExecutionEnvironment()
					.fromElements(1)
					.mapPartition(new MapPartitionFunction<Integer, Row>() {
						@Override
						public void mapPartition(Iterable<Integer> values, Collector<Row> out) throws Exception {
							//pass
						}
					}),
				emptyModel.getModelSchema().getFieldNames(),
				emptyModel.getModelSchema().getFieldTypes()
			).setMLEnvironmentId(input.getMLEnvironmentId());
		}

		return quantileDiscretizerModel;
	}

	public static BatchOperator<?> castToQuantile(
		BatchOperator<?> input,
		BatchOperator<?> quantileDiscretizerModel,
		Params params) {
		String[] continuousColNames = ArrayUtils.removeElements(
			params.get(HasFeatureCols.FEATURE_COLS),
			params.get(HasCategoricalCols.CATEGORICAL_COLS)
		);

		if (continuousColNames != null && continuousColNames.length > 0) {
			input = new QuantileDiscretizerPredictBatchOp()
				.setMLEnvironmentId(input.getMLEnvironmentId())
				.setSelectedCols(continuousColNames)
				.linkFrom(quantileDiscretizerModel, input);
			input = new NumericalTypeCastBatchOp()
				.setMLEnvironmentId(input.getMLEnvironmentId())
				.setSelectedCols(continuousColNames)
				.setTargetType("INT")
				.linkFrom(input);
		}

		return input;
	}

	public static BatchOperator<?> sample(BatchOperator<?> inputOp, Params params) {
		DataSet<Row> input = inputOp.getDataSet();

		DataSet<Long> count = DataSetUtils.countElementsPerPartition(input)
			.sum(1).map(new MapFunction<Tuple2<Integer, Long>, Long>() {
				@Override
				public Long map(Tuple2<Integer, Long> value) throws Exception {
					return value.f1;
				}
			});

		final long seed = System.currentTimeMillis();
		final long sampleCount = params.get(SAMPLE_COUNT_4_BIN);

		return new DataSetWrapperBatchOp(
			input
				.mapPartition(new RichMapPartitionFunction<Row, Row>() {
					double ratio;

					Random random;

					@Override
					public void open(Configuration parameters) throws Exception {
						long count = (long) getRuntimeContext().getBroadcastVariable("totalCount").get(0);

						ratio = Math.min((double) sampleCount / (double) count, 1.0);

						random = new Random(seed + getRuntimeContext().getIndexOfThisSubtask());
					}

					@Override
					public void mapPartition(Iterable<Row> values, Collector<Row> out) throws Exception {
						for (Row r : values) {
							if (random.nextDouble() < ratio) {
								out.collect(r);
							}
						}
					}
				})
				.withBroadcastSet(count, "totalCount"),
			inputOp.getColNames(),
			inputOp.getColTypes()
		).setMLEnvironmentId(inputOp.getMLEnvironmentId());
	}
}
