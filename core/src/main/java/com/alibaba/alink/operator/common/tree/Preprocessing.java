package com.alibaba.alink.operator.common.tree;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOModelMapper;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.operator.common.dataproc.SortUtilsNext;
import com.alibaba.alink.operator.common.feature.ContinuousRanges;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper;
import com.alibaba.alink.operator.common.feature.quantile.PairComparable;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.dataproc.HasStringOrderTypeDefaultAsRandom;
import com.alibaba.alink.params.feature.HasDropLast;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoe;
import com.alibaba.alink.params.feature.QuantileDiscretizerTrainParams;
import com.alibaba.alink.params.mapper.SISOMapperParams;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.tree.HasMaxBins;
import com.alibaba.alink.params.statistics.HasRoundMode;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;

/**
 * Util class for data pre-processing in the random forest.
 */
public final class Preprocessing {

	public static final ParamInfo <Boolean> ZERO_AS_MISSING
		= ParamInfoFactory.createParamInfo("zeroAsMissing", Boolean.class)
		.setHasDefaultValue(false)
		.build();

	private static final Logger LOG = LoggerFactory.getLogger(Preprocessing.class);

	/**
	 * Sample count that be used in the quantile discretizer.
	 */
	public final static ParamInfo <Long> SAMPLE_COUNT_4_BIN = ParamInfoFactory
		.createParamInfo("sampleCount4Bin", Long.class)
		.setHasDefaultValue(500000L)
		.build();

	/**
	 * Distinct the label column and get the dataset of unique labels.
	 *
	 * @param input the training data.
	 * @return the dataset of unique label.
	 */
	public static DataSet <Object[]> distinctLabels(DataSet <Object> input) {
		return input
			.map(new MapFunction <Object, Tuple1 <Comparable>>() {
				private static final long serialVersionUID = -6913787844845900748L;

				@Override
				public Tuple1 <Comparable> map(Object value) throws Exception {
					return Tuple1.of((Comparable) value);
				}
			})
			.groupBy(0)
			.reduce(new ReduceFunction <Tuple1 <Comparable>>() {
				private static final long serialVersionUID = -9106561855242251475L;

				@Override
				public Tuple1 <Comparable> reduce(Tuple1 <Comparable> in1, Tuple1 <Comparable> in2) throws Exception {
					return in1;
				}
			})
			.reduceGroup(new GroupReduceFunction <Tuple1 <Comparable>, Object[]>() {
				private static final long serialVersionUID = -6987738021646631957L;

				@Override
				public void reduce(Iterable <Tuple1 <Comparable>> values, Collector <Object[]> out) throws Exception {
					LOG.info("distinctLabels start");
					out.collect(
						StreamSupport.stream(values.spliterator(), false)
							.map(x -> x.f0)
							.sorted(new SortUtils.ComparableComparator())
							.toArray(Object[]::new)
					);
					LOG.info("distinctLabels end");
				}
			});
	}

	/**
	 * Transform the label column as Integer.
	 */
	public static DataSet <Row> findIndexOfLabel(DataSet <Row> input, DataSet <Object[]> model, final int colIdx) {
		return input
			.map(new RichMapFunction <Row, Row>() {
				private static final long serialVersionUID = -5365735281787702408L;
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
			throw new AkIllegalArgumentException("Can not find " + val);
		}
	}

	public static DataSet <Object[]> generateLabels(
		BatchOperator <?> input,
		Params params,
		boolean isRegression) {
		DataSet <Object[]> labels;
		if (!isRegression) {
			final String labelColName = params.get(HasLabelCol.LABEL_COL);
			DataSet <Row> labelDataSet = select(input, labelColName).getDataSet();

			labels = distinctLabels(labelDataSet
				.map(new MapFunction <Row, Object>() {
					private static final long serialVersionUID = -3394717275759972231L;

					@Override
					public Object map(Row value) {
						return value.getField(0);
					}
				})
			);

		} else {
			labels = MLEnvironmentFactory
				.get(input.getMLEnvironmentId())
				.getExecutionEnvironment()
				.fromElements(1)
				.mapPartition(new MapPartitionFunction <Integer, Object[]>() {
					private static final long serialVersionUID = 475582663950451641L;

					@Override
					public void mapPartition(Iterable <Integer> values, Collector <Object[]> out) {
						//pass
					}
				});
		}

		return labels;
	}

	public static BatchOperator <?> castLabel(
		BatchOperator <?> input, Params params, DataSet <Object[]> labels, boolean isRegression) {
		String[] inputColNames = input.getColNames();
		if (!isRegression) {
			final String labelColName = params.get(HasLabelCol.LABEL_COL);
			final TypeInformation <?>[] types = input.getColTypes();
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
				input = new NumericalTypeCast()
					.setMLEnvironmentId(input.getMLEnvironmentId())
					.setSelectedCols(params.get(HasLabelCol.LABEL_COL))
					.setTargetType("DOUBLE")
					.linkFrom(input);
			}
		}

		return input;
	}

	public static BatchOperator <?> generateStringIndexerModel(BatchOperator <?> input, Params params) {
		String[] categoricalColNames = null;
		if (params.contains(HasCategoricalCols.CATEGORICAL_COLS)) {
			categoricalColNames = params.get(HasCategoricalCols.CATEGORICAL_COLS);
		}
		BatchOperator <?> stringIndexerModel;
		if (categoricalColNames == null || categoricalColNames.length == 0) {
			MultiStringIndexerModelDataConverter emptyModel = new MultiStringIndexerModelDataConverter();

			stringIndexerModel = new DataSetWrapperBatchOp(
				MLEnvironmentFactory
					.get(input.getMLEnvironmentId())
					.getExecutionEnvironment()
					.fromElements(1)
					.mapPartition(new MapPartitionFunction <Integer, Row>() {
						private static final long serialVersionUID = -7481931851291494026L;

						@Override
						public void mapPartition(Iterable <Integer> values, Collector <Row> out) throws Exception {
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
				.setStringOrderType(HasStringOrderTypeDefaultAsRandom.StringOrderType.ALPHABET_ASC)
				.linkFrom(input);
		}

		return stringIndexerModel;
	}

	public static BatchOperator <?> castCategoricalCols(
		BatchOperator <?> input,
		BatchOperator <?> stringIndexerModel,
		Params params) {
		String[] categoricalColNames = params.get(HasCategoricalCols.CATEGORICAL_COLS);
		if (categoricalColNames != null && categoricalColNames.length != 0) {
			input = select(
				new MultiStringIndexerPredictBatchOp()
					.setMLEnvironmentId(input.getMLEnvironmentId())
					.setHandleInvalid("skip")
					.setSelectedCols(categoricalColNames)
					.setReservedCols(input.getColNames())
					.linkFrom(stringIndexerModel, input),
				input.getColNames()
			);
			input = new NumericalTypeCast()
				.setMLEnvironmentId(input.getMLEnvironmentId())
				.setSelectedCols(categoricalColNames)
				.setTargetType("INT")
				.linkFrom(input);
		}

		return input;
	}

	public static BatchOperator <?> castContinuousCols(
		BatchOperator <?> input,
		Params params) {

		String[] continuousColNames;

		if (params.contains(HasCategoricalCols.CATEGORICAL_COLS)) {
			continuousColNames = ArrayUtils
				.removeElements(
					params.get(HasFeatureCols.FEATURE_COLS),
					params.get(HasCategoricalCols.CATEGORICAL_COLS)
				);
		} else {
			continuousColNames = params.get(HasFeatureCols.FEATURE_COLS);
		}

		if (continuousColNames != null && continuousColNames.length > 0) {
			input = new NumericalTypeCast()
				.setMLEnvironmentId(input.getMLEnvironmentId())
				.setSelectedCols(continuousColNames)
				.setTargetType("DOUBLE")
				.linkFrom(input);
		}

		return input;
	}

	public static BatchOperator <?> castWeightCol(
		BatchOperator <?> input,
		Params params) {
		String weightCol = params.get(HasWeightColDefaultAsNull.WEIGHT_COL);
		if (weightCol == null) {
			return input;
		}

		return new NumericalTypeCast()
			.setMLEnvironmentId(input.getMLEnvironmentId())
			.setSelectedCols(weightCol)
			.setTargetType("DOUBLE")
			.linkFrom(input);
	}

	public static BatchOperator <?> generateQuantileDiscretizerModel(
		BatchOperator <?> input,
		Params params) {
		if (params.contains(HasVectorColDefaultAsNull.VECTOR_COL)) {
			return sample(input, params)
				.linkTo(new VectorTrain(
					new Params().set(ZERO_AS_MISSING, params.get(ZERO_AS_MISSING)))
					.setMLEnvironmentId(input.getMLEnvironmentId())
					.setVectorCol(params.get(HasVectorColDefaultAsNull.VECTOR_COL))
					.setNumBuckets(params.get(HasMaxBins.MAX_BINS))
				);
		}

		String[] continuousColNames = ArrayUtils.removeElements(
			params.get(HasFeatureCols.FEATURE_COLS),
			params.get(HasCategoricalCols.CATEGORICAL_COLS)
		);
		BatchOperator <?> quantileDiscretizerModel;
		if (continuousColNames != null && continuousColNames.length > 0) {
			quantileDiscretizerModel = sample(input, params)
				.linkTo(new QuantileDiscretizerTrainBatchOp(
					new Params().set(ZERO_AS_MISSING, params.get(ZERO_AS_MISSING)))
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
					.mapPartition(new MapPartitionFunction <Integer, Row>() {
						private static final long serialVersionUID = 2328781103352773618L;

						@Override
						public void mapPartition(Iterable <Integer> values, Collector <Row> out) throws Exception {
							//pass
						}
					}),
				emptyModel.getModelSchema().getFieldNames(),
				emptyModel.getModelSchema().getFieldTypes()
			).setMLEnvironmentId(input.getMLEnvironmentId());
		}

		return quantileDiscretizerModel;
	}

	public static BatchOperator <?> castToQuantile(
		BatchOperator <?> input,
		BatchOperator <?> quantileDiscretizerModel,
		Params params) {

		if (params.contains(HasVectorColDefaultAsNull.VECTOR_COL)) {
			input = new VectorPredict()
				.setMLEnvironmentId(input.getMLEnvironmentId())
				.setVectorCol(params.get(HasVectorColDefaultAsNull.VECTOR_COL))
				.linkFrom(quantileDiscretizerModel, input);

			return input;
		}

		String[] continuousColNames = ArrayUtils.removeElements(
			params.get(HasFeatureCols.FEATURE_COLS),
			params.get(HasCategoricalCols.CATEGORICAL_COLS)
		);

		if (continuousColNames != null && continuousColNames.length > 0) {
			input = new QuantileDiscretizerPredictBatchOp()
				.setMLEnvironmentId(input.getMLEnvironmentId())
				.setSelectedCols(continuousColNames)
				.setHandleInvalid("SKIP")
				.linkFrom(quantileDiscretizerModel, input);
			input = new NumericalTypeCast()
				.setMLEnvironmentId(input.getMLEnvironmentId())
				.setSelectedCols(continuousColNames)
				.setTargetType("INT")
				.linkFrom(input);
		}

		return input;
	}

	public static BatchOperator <?> sample(BatchOperator <?> inputOp, Params params) {
		DataSet <Row> input = inputOp.getDataSet();

		DataSet <Long> count = DataSetUtils.countElementsPerPartition(input)
			.sum(1).map(new MapFunction <Tuple2 <Integer, Long>, Long>() {
				private static final long serialVersionUID = -8942137921419703888L;

				@Override
				public Long map(Tuple2 <Integer, Long> value) throws Exception {
					return value.f1;
				}
			});

		final long seed = System.currentTimeMillis();
		final long sampleCount = params.get(SAMPLE_COUNT_4_BIN);

		return new DataSetWrapperBatchOp(
			input
				.mapPartition(new RichMapPartitionFunction <Row, Row>() {
					private static final long serialVersionUID = -68193902220003941L;
					double ratio;

					Random random;

					@Override
					public void open(Configuration parameters) throws Exception {
						long count = (long) getRuntimeContext().getBroadcastVariable("totalCount").get(0);

						ratio = Math.min((double) sampleCount / (double) count, 1.0);

						random = new Random(seed + getRuntimeContext().getIndexOfThisSubtask());
						LOG.info("{} open.", getRuntimeContext().getTaskName());
					}

					@Override
					public void close() throws Exception {
						super.close();
						LOG.info("{} close.", getRuntimeContext().getTaskName());
					}

					@Override
					public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
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

	public static BatchOperator <?> select(BatchOperator <?> in, String... selectCols) {
		final int[] selectIndices = TableUtil.findColIndicesWithAssertAndHint(in.getColNames(), selectCols);
		final TypeInformation <?>[] selectColTypes = TableUtil.findColTypesWithAssertAndHint(in.getSchema(),
			selectCols);

		return new TableSourceBatchOp(
			DataSetConversionUtil.toTable(
				in.getMLEnvironmentId(),
				in
					.getDataSet()
					.map(new RichMapFunction <Row, Row>() {
							 private static final long serialVersionUID = 9119490369706910594L;

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
							 public Row map(Row value) throws Exception {
								 Row ret = new Row(selectIndices.length);
								 for (int i = 0; i < selectIndices.length; ++i) {
									 ret.setField(i, value.getField(selectIndices[i]));
								 }

								 return ret;
							 }
						 }
					),
				selectCols,
				selectColTypes
			)
		).setMLEnvironmentId(in.getMLEnvironmentId());
	}

	public static boolean isMissing(
		Object val, boolean isNumber, boolean zeroAsMissing) {

		return val == null || (isNumber && isMissing(((Number) val).doubleValue(), zeroAsMissing));
	}

	public static boolean isMissing(
		Object val, int missingIndex) {

		return val == null || (int) val == missingIndex;
	}

	public static boolean isMissing(double val, boolean zeroAsMissing) {
		return (zeroAsMissing && val == 0.0) || Double.isNaN(val);
	}

	public static boolean isMissing(long val, boolean zeroAsMissing) {
		return zeroAsMissing && val == 0L;
	}

	public static boolean isMissing(double val, FeatureMeta featureMeta, boolean zeroAsMissing) {
		if (featureMeta.getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
			return isMissing(val, zeroAsMissing);
		} else {
			return isMissing((int) val, featureMeta.getMissingIndex());
		}
	}

	public static boolean isMissing(Object val, FeatureMeta featureMeta, boolean zeroAsMissing) {
		if (featureMeta.getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
			return isMissing(val, true, zeroAsMissing);
		} else {
			return isMissing(val, featureMeta.getMissingIndex());
		}
	}

	public static boolean isSparse(Params params) {
		return params.contains(HasVectorColDefaultAsNull.VECTOR_COL);
	}

	public static <T> String[] checkAndGetOptionalFeatureCols(Params params, T that) {
		if (params.contains(HasFeatureColsDefaultAsNull.FEATURE_COLS)) {
			return params.get(HasFeatureColsDefaultAsNull.FEATURE_COLS);
		}

		throw new AkIllegalArgumentException("Could not find the feature columns. "
			+ "Please consider to set the feature columns on the "
			+ that.getClass().getName()
			+ ".");
	}

	public static <T> String checkAndGetOptionalVectorCols(Params params, T that) {
		if (params.contains(HasVectorColDefaultAsNull.VECTOR_COL)) {
			return params.get(HasVectorColDefaultAsNull.VECTOR_COL);
		}

		throw new AkIllegalArgumentException("Could not find the vector column. "
			+ "Please consider to set the vector column on the "
			+ that.getClass().getName()
			+ ".");
	}

	public static int zeroIndex(QuantileDiscretizerModelDataConverter model, String featureName) {
		return createVectorDiscretizer(
			model.data.get(featureName),
			model.meta.get(ZERO_AS_MISSING)
		).findIndex(0.0);
	}

	private static VectorDiscretizer createVectorDiscretizer(
		ContinuousRanges continuousRanges, boolean zeroAsMissing) {
		if (continuousRanges.isFloat()) {
			int size = continuousRanges.splitsArray.length + 1;
			boolean isLeftOpen = continuousRanges.getLeftOpen();
			int nullIndex = continuousRanges.getIntervalNum();

			int[] boundIndex = IntStream.range(0, size + 2).toArray();
			boundIndex[size] = size - 1;

			double[] bounds = new double[size + 1];
			bounds[0] = Double.NEGATIVE_INFINITY;
			for (int i = 0; i < size - 1; ++i) {
				bounds[i + 1] = continuousRanges.splitsArray[i].doubleValue();
			}
			bounds[size] = Double.POSITIVE_INFINITY;

			return new VectorDiscretizer(bounds, isLeftOpen, boundIndex, nullIndex, zeroAsMissing);
		} else {
			throw new UnsupportedOperationException("Unsupported now.");
		}
	}

	private static DataSet <Row> toVectorModel(
		DataSet <Row> input,
		final int quantileNum,
		final HasRoundMode.RoundMode roundMode,
		final boolean zeroAsMissing) {

		/* instance count of dataset */
		DataSet <Long> totalCounts = DataSetUtils.countElementsPerPartition(input)
			.sum(1)
			.map(new MapFunction <Tuple2 <Integer, Long>, Long>() {
				private static final long serialVersionUID = 2858799989301224611L;

				@Override
				public Long map(Tuple2 <Integer, Long> value) throws Exception {
					return value.f1;
				}
			});

		/* missing count of columns */
		DataSet <Tuple2 <Integer, Long>> missingCounts = input
			.mapPartition(new RichMapPartitionFunction <Row, Tuple2 <Integer, Long>>() {
				private static final long serialVersionUID = 1205331017481743252L;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					VectorTrain.LOG.info("{} open.", getRuntimeContext().getTaskName());
				}

				@Override
				public void close() throws Exception {
					super.close();
					VectorTrain.LOG.info("{} close.", getRuntimeContext().getTaskName());
				}

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, Long>> out) {
					Map <Integer, Long> missingMap = new HashMap <>();

					for (Row val : values) {
						Vector vector = VectorUtil.getVector(val.getField(0));

						if (vector instanceof SparseVector) {
							SparseVector sparseVector = (SparseVector) vector;

							int[] indices = sparseVector.getIndices();
							double[] vals = sparseVector.getValues();

							for (int i = 0; i < indices.length; ++i) {
								if (isMissing(vals[i], zeroAsMissing)) {
									missingMap.merge(indices[i], 1L, Long::sum);
								}
							}
						} else {
							DenseVector denseVector = (DenseVector) vector;

							double[] vals = denseVector.getData();

							for (int i = 0; i < vals.length; ++i) {
								if (isMissing(vals[i], zeroAsMissing)) {
									missingMap.merge(i, 1L, Long::sum);
								}
							}
						}
					}

					for (Map.Entry <Integer, Long> entry : missingMap.entrySet()) {
						out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
					}
				}
			})
			.groupBy(0)
			.reduce(new RichReduceFunction <Tuple2 <Integer, Long>>() {
				private static final long serialVersionUID = -2194135190247682594L;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					VectorTrain.LOG.info("{} open.", getRuntimeContext().getTaskName());
				}

				@Override
				public void close() throws Exception {
					super.close();
					VectorTrain.LOG.info("{} close.", getRuntimeContext().getTaskName());
				}

				@Override
				public Tuple2 <Integer, Long> reduce(Tuple2 <Integer, Long> value1, Tuple2 <Integer, Long> value2) {
					return Tuple2.of(value1.f0, value1.f1 + value2.f1);
				}
			});

		DataSet <Tuple2 <Integer, Long>> nonzeroCounts = input
			.mapPartition(new RichMapPartitionFunction <Row, Tuple2 <Integer, Long>>() {
				private static final long serialVersionUID = -4514110269239741447L;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					VectorTrain.LOG.info("{} open.", getRuntimeContext().getTaskName());
				}

				@Override
				public void close() throws Exception {
					super.close();
					VectorTrain.LOG.info("{} close.", getRuntimeContext().getTaskName());
				}

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, Long>> out) {
					Map <Integer, Long> nonzeroCnt = new HashMap <>();

					for (Row val : values) {
						Vector vector = VectorUtil.getVector(val.getField(0));

						if (vector instanceof SparseVector) {
							SparseVector sparseVector = (SparseVector) vector;

							int[] indices = sparseVector.getIndices();

							for (int index : indices) {
								nonzeroCnt.merge(index, 1L, Long::sum);
							}
						} else {
							DenseVector denseVector = (DenseVector) vector;

							double[] vals = denseVector.getData();

							for (int i = 0; i < vals.length; ++i) {
								nonzeroCnt.merge(i, 1L, Long::sum);
							}
						}
					}

					for (Map.Entry <Integer, Long> entry : nonzeroCnt.entrySet()) {
						out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
					}
				}
			})
			.groupBy(0)
			.reduce(new RichReduceFunction <Tuple2 <Integer, Long>>() {
				private static final long serialVersionUID = 4055269324161485854L;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					VectorTrain.LOG.info("{} open.", getRuntimeContext().getTaskName());
				}

				@Override
				public void close() throws Exception {
					super.close();
					VectorTrain.LOG.info("{} close.", getRuntimeContext().getTaskName());
				}

				@Override
				public Tuple2 <Integer, Long> reduce(Tuple2 <Integer, Long> value1, Tuple2 <Integer, Long> value2) {
					return Tuple2.of(value1.f0, value1.f1 + value2.f1);
				}
			});

		DataSet <Tuple2 <Integer, Long>> lessZeroCounts = input
			.mapPartition(new RichMapPartitionFunction <Row, Tuple2 <Integer, Long>>() {
				private static final long serialVersionUID = -3948602601761057786L;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					VectorTrain.LOG.info("{} open.", getRuntimeContext().getTaskName());
				}

				@Override
				public void close() throws Exception {
					super.close();
					VectorTrain.LOG.info("{} close.", getRuntimeContext().getTaskName());
				}

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, Long>> out) {
					Map <Integer, Long> lessZero = new HashMap <>();

					for (Row val : values) {
						Vector vector = VectorUtil.getVector(val.getField(0));

						if (vector instanceof SparseVector) {
							SparseVector sparseVector = (SparseVector) vector;

							int[] indices = sparseVector.getIndices();
							double[] vals = sparseVector.getValues();

							for (int i = 0; i < indices.length; ++i) {
								if (!isMissing(vals[i], zeroAsMissing) && vals[i] < 0) {
									lessZero.merge(indices[i], 1L, Long::sum);
								}
							}
						} else {
							DenseVector denseVector = (DenseVector) vector;

							double[] vals = denseVector.getData();

							for (int i = 0; i < vals.length; ++i) {
								if (!isMissing(vals[i], zeroAsMissing) && vals[i] < 0) {
									lessZero.merge(i, 1L, Long::sum);
								}
							}
						}
					}

					for (Map.Entry <Integer, Long> entry : lessZero.entrySet()) {
						out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
					}
				}
			})
			.groupBy(0)
			.reduce(new RichReduceFunction <Tuple2 <Integer, Long>>() {
				private static final long serialVersionUID = -2353281797919459915L;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					VectorTrain.LOG.info("{} open.", getRuntimeContext().getTaskName());
				}

				@Override
				public void close() throws Exception {
					super.close();
					VectorTrain.LOG.info("{} close.", getRuntimeContext().getTaskName());
				}

				@Override
				public Tuple2 <Integer, Long> reduce(Tuple2 <Integer, Long> value1, Tuple2 <Integer, Long> value2) {
					return Tuple2.of(value1.f0, value1.f1 + value2.f1);
				}
			});

		DataSet <PairComparable> flatten = input
			.mapPartition(new RichMapPartitionFunction <Row, PairComparable>() {

				private static final long serialVersionUID = 4270417871029343805L;
				PairComparable pairBuff;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					VectorTrain.LOG.info("{} open.", getRuntimeContext().getTaskName());
					pairBuff = new PairComparable();
				}

				@Override
				public void close() throws Exception {
					super.close();
					VectorTrain.LOG.info("{} close.", getRuntimeContext().getTaskName());
				}

				@Override
				public void mapPartition(Iterable <Row> values, Collector <PairComparable> out) {
					for (Row value : values) {
						Vector vector = VectorUtil.getVector(value.getField(0));

						if (vector instanceof SparseVector) {
							SparseVector sparseVector = (SparseVector) vector;

							int[] indices = sparseVector.getIndices();
							double[] vals = sparseVector.getValues();

							for (int i = 0; i < indices.length; ++i) {
								pairBuff.first = indices[i];
								pairBuff.second = isMissing(vals[i], zeroAsMissing) ? null : vals[i];
								out.collect(pairBuff);
							}
						} else {
							DenseVector denseVector = (DenseVector) vector;

							double[] vals = denseVector.getData();

							for (int i = 0; i < vals.length; ++i) {
								pairBuff.first = i;
								pairBuff.second = isMissing(vals[i], zeroAsMissing) ? null : vals[i];
								out.collect(pairBuff);
							}
						}
					}
				}
			});

		Tuple2 <DataSet <PairComparable>, DataSet <Tuple2 <Integer, Long>>> sortedData
			= SortUtilsNext.pSort(flatten);

		return sortedData.f0
			.mapPartition(new MultiVector(quantileNum, roundMode, zeroAsMissing))
			.withBroadcastSet(sortedData.f1, "partitionedCounts")
			.withBroadcastSet(totalCounts, "totalCounts")
			.withBroadcastSet(missingCounts, "missingCounts")
			.withBroadcastSet(nonzeroCounts, "nonzeroCounts")
			.withBroadcastSet(lessZeroCounts, "lessZeroCounts")
			.groupBy(0)
			.reduceGroup(new RichGroupReduceFunction <Tuple2 <Integer, Number>, Row>() {
				private static final long serialVersionUID = 1349754303677527939L;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					VectorTrain.LOG.info("{} open.", getRuntimeContext().getTaskName());
				}

				@Override
				public void close() throws Exception {
					super.close();
					VectorTrain.LOG.info("{} close.", getRuntimeContext().getTaskName());
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
			});
	}

	private static final class VectorPredict
		extends ModelMapBatchOp <VectorPredict>
		implements VectorPredictParams <VectorPredict> {

		private static final long serialVersionUID = -9334571162681404L;

		public VectorPredict() {
			this(null);
		}

		public VectorPredict(Params params) {
			super(VectorModel::new, params);
		}
	}

	private interface VectorPredictParams<T> extends
		HasVectorCol <T>,
		HasReservedColsDefaultAsNull <T>,
		HasOutputColDefaultAsNull <T>,
		HasHandleInvalid <T>,
		HasEncodeWithoutWoe <T>,
		HasDropLast <T> {
	}

	private static class VectorModel extends SISOModelMapper {
		private static final long serialVersionUID = 4501962799112695132L;

		private final Map <Integer, VectorDiscretizer> discretizers = new HashMap <>();

		public VectorModel(TableSchema modelSchema, TableSchema dataSchema, Params params) {
			super(modelSchema, dataSchema,
				params.set(SISOMapperParams.SELECTED_COL, params.get(VectorPredictParams.VECTOR_COL)));
		}

		@Override
		protected TypeInformation <?> initPredResultColType() {
			return AlinkTypes.VECTOR;
		}

		@Override
		protected Object predictResult(Object input) throws Exception {
			Vector vector = VectorUtil.getVector(input);

			if (vector instanceof SparseVector) {
				SparseVector sparseVector = (SparseVector) vector;

				int[] indices = sparseVector.getIndices();
				double[] vals = sparseVector.getValues();

				for (int i = 0; i < indices.length; ++i) {
					VectorDiscretizer discretizer = discretizers.get(indices[i]);

					if (discretizer == null) {
						vals[i] = 0.0;
					} else {
						vals[i] = discretizer.findIndex(vals[i]);
					}
				}
			} else {
				DenseVector denseVector = (DenseVector) vector;

				double[] data = denseVector.getData();

				for (int i = 0; i < data.length; ++i) {
					VectorDiscretizer discretizer = discretizers.get(i);
					if (discretizer == null) {
						throw new IllegalArgumentException(
							String.format("Can not find the discretizer for indices: %d", i)
						);
					}
					data[i] = discretizer.findIndex(data[i]);
				}
			}

			return vector;
		}

		@Override
		public void loadModel(List <Row> modelRows) {
			QuantileDiscretizerModelDataConverter model = new QuantileDiscretizerModelDataConverter();
			model.load(modelRows);

			for (Map.Entry <String, ContinuousRanges> entry : model.data.entrySet()) {
				discretizers.put(
					Integer.valueOf(entry.getKey()),
					createVectorDiscretizer(entry.getValue(), model.meta.get(ZERO_AS_MISSING))
				);
			}
		}

	}

	private static class VectorDiscretizer
		implements QuantileDiscretizerModelMapper.NumericQuantileDiscretizer {

		private static final long serialVersionUID = -5893784530000492957L;
		double[] bounds;
		boolean isLeftOpen;
		int[] boundIndex;
		int nullIndex;
		boolean zeroAsMissing;

		public VectorDiscretizer(
			double[] bounds, boolean isLeftOpen, int[] boundIndex, int nullIndex, boolean zeroAsMissing) {
			this.bounds = bounds;
			this.isLeftOpen = isLeftOpen;
			this.boundIndex = boundIndex;
			this.nullIndex = nullIndex;
			this.zeroAsMissing = zeroAsMissing;
		}

		private int findIndexInner(double number) {
			int hit = Arrays.binarySearch(bounds, number);

			if (isLeftOpen) {
				hit = hit >= 0 ? hit - 1 : -hit - 2;
			} else {
				hit = hit >= 0 ? hit : -hit - 2;
			}

			return hit;
		}

		@Override
		public boolean isValid(int index) {
			return index != nullIndex;
		}

		@Override
		public int findIndex(Object number) {
			if (isMissing((Double) number, zeroAsMissing)) {
				return nullIndex;
			}

			int hit = findIndexInner(((Number) number).doubleValue());

			return boundIndex[hit];
		}
	}

	private static final class VectorTrain extends BatchOperator <VectorTrain>
		implements QuantileDiscretizerTrainParams <VectorTrain>,
		HasVectorColDefaultAsNull <VectorTrain> {

		private static final Logger LOG = LoggerFactory.getLogger(VectorTrain.class);
		private static final long serialVersionUID = -1589056627883942993L;

		public VectorTrain() {
			this(null);
		}

		public VectorTrain(Params params) {
			super(params);
		}

		@Override
		public VectorTrain linkFrom(BatchOperator <?>... inputs) {
			BatchOperator <?> in = checkAndGetFirst(inputs);
			if (getParams().contains(QuantileDiscretizerTrainParams.NUM_BUCKETS) && getParams().contains(
				QuantileDiscretizerTrainParams.NUM_BUCKETS_ARRAY)) {
				throw new AkIllegalArgumentException(
					"It can not set num_buckets and num_buckets_array at the same time."
				);
			}

			String vectorColName = getVectorCol();
			int quantileNum = getNumBuckets();

			/* filter the selected column from input */
			DataSet <Row> input = Preprocessing.select(in, vectorColName).getDataSet();

			DataSet <Row> quantile = toVectorModel(
				input,
				quantileNum,
				getParams().get(HasRoundMode.ROUND_MODE),
				getParams().get(ZERO_AS_MISSING)
			);

			quantile = quantile.reduceGroup(
				new Preprocessing.SerializeModel(
					getParams()
				)
			);

			/* set output */
			setOutput(quantile, new QuantileDiscretizerModelDataConverter().getModelSchema());

			return this;
		}

	}

	private static class SerializeModel implements GroupReduceFunction <Row, Row> {
		private static final long serialVersionUID = -3408433803135796522L;
		private final Params meta;

		public SerializeModel(Params meta) {
			this.meta = meta;
		}

		@Override
		public void reduce(Iterable <Row> values, Collector <Row> out) throws Exception {
			Map <String, ContinuousRanges> m = new HashMap <>();
			for (Row val : values) {
				int index = (int) val.getField(0);
				Number[] splits = (Number[]) val.getField(1);
				m.put(
					String.valueOf(index),
					QuantileDiscretizerModelDataConverter.arraySplit2ContinuousRanges(
						String.valueOf(index),
						Types.DOUBLE,
						splits,
						meta.get(QuantileDiscretizerTrainParams.LEFT_OPEN)
					)
				);
			}

			QuantileDiscretizerModelDataConverter model = new QuantileDiscretizerModelDataConverter(m, meta);

			model.save(model, out);
		}
	}

	private static class MultiVector
		extends RichMapPartitionFunction <PairComparable, Tuple2 <Integer, Number>> {

		private static final Logger LOG = LoggerFactory.getLogger(MultiVector.class);
		private static final long serialVersionUID = 8462783213350057361L;
		final private int quantileNum;
		final private HasRoundMode.RoundMode roundType;
		final private boolean zeroAsMissing;

		private transient int taskId;
		private transient long totalCounts = 0;
		private transient List <Tuple2 <Integer, Long>> partitionedCounts;
		private transient List <Tuple2 <Integer, Long>> missingCounts;
		private transient List <Tuple2 <Integer, Long>> lessZeroCounts;
		private transient List <Tuple2 <Integer, Long>> nonzeroCounts;
		private transient List <Tuple2 <Integer, Long>> nonzeroOffsets;

		public MultiVector(int quantileNum, HasRoundMode.RoundMode roundType, boolean zeroAsMissing) {
			this.quantileNum = quantileNum;
			this.roundType = roundType;
			this.zeroAsMissing = zeroAsMissing;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.partitionedCounts = getRuntimeContext().getBroadcastVariableWithInitializer(
				"partitionedCounts",
				new BroadcastVariableInitializer <Tuple2 <Integer, Long>, List <Tuple2 <Integer, Long>>>() {
					@Override
					public List <Tuple2 <Integer, Long>> initializeBroadcastVariable(
						Iterable <Tuple2 <Integer, Long>> data) {
						ArrayList <Tuple2 <Integer, Long>> sortedData = new ArrayList <>();
						for (Tuple2 <Integer, Long> datum : data) {
							sortedData.add(datum);
						}

						sortedData.sort(comparing(o -> o.f0));

						return sortedData;
					}
				});

			this.totalCounts = getRuntimeContext().getBroadcastVariableWithInitializer("totalCounts",
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
							.sorted(comparing(o -> o.f0))
							.collect(Collectors.toList());
					}
				}
			);

			this.lessZeroCounts = getRuntimeContext().getBroadcastVariableWithInitializer(
				"lessZeroCounts",
				new BroadcastVariableInitializer <Tuple2 <Integer, Long>, List <Tuple2 <Integer, Long>>>() {
					@Override
					public List <Tuple2 <Integer, Long>> initializeBroadcastVariable(
						Iterable <Tuple2 <Integer, Long>> data) {
						return StreamSupport.stream(data.spliterator(), false)
							.sorted(comparing(o -> o.f0))
							.collect(Collectors.toList());
					}
				}
			);

			this.nonzeroCounts = getRuntimeContext().getBroadcastVariableWithInitializer(
				"nonzeroCounts",
				new BroadcastVariableInitializer <Tuple2 <Integer, Long>, List <Tuple2 <Integer, Long>>>() {
					@Override
					public List <Tuple2 <Integer, Long>> initializeBroadcastVariable(
						Iterable <Tuple2 <Integer, Long>> data) {
						return StreamSupport.stream(data.spliterator(), false)
							.sorted(comparing(o -> o.f0))
							.collect(Collectors.toList());
					}
				}
			);

			nonzeroOffsets = new ArrayList <>(nonzeroCounts.size());

			long offset = 0;
			for (Tuple2 <Integer, Long> nonzero : nonzeroCounts) {
				nonzeroOffsets.add(Tuple2.of(nonzero.f0, offset));
				offset += nonzero.f1;
			}

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

			long start = 0;
			long end;

			int curListIndex = -1;
			int size = partitionedCounts.size();

			for (int i = 0; i < size; ++i) {
				int curId = partitionedCounts.get(i).f0;

				if (curId == taskId) {
					curListIndex = i;
					break;
				}

				if (curId > taskId) {
					throw new AkUnclassifiedErrorException("Error curId: " + curId
						+ ". id: " + taskId);
				}

				start += partitionedCounts.get(i).f1;
			}

			end = start + partitionedCounts.get(curListIndex).f1;

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

			allRows.sort(naturalOrder());

			int excludeEndFeature = featureOffset(featureIndexInOffsets(end)) == end ?
				featureIndexInOffsets(end) : featureIndexInOffsets(end) + 1;

			size = excludeEndFeature - featureIndexInOffsets(start);

			int localStart = 0;

			for (int i = 0; i < size; ++i) {
				int featureIndex = featureIndexInOffsets(start) + i;
				int subStart = 0;
				int subEnd = (int) nonzeroCount(featureIndex);

				if (i == 0) {
					subStart = (int) (start - featureOffset(featureIndex));
				}

				if (i == size - 1) {
					subEnd = (int) (end - featureOffset(featureIndex));
				}

				long notMissingCount = notMissingCount(featureIndex);
				long lessZeroCount = lessZeroCount(featureIndex);
				long zeroCount = zeroCount(featureIndex);

				if (zeroAsMissing) {
					notMissingCount -= zeroCount;

					QuantileDiscretizerTrainBatchOp.QIndex qIndex = new QuantileDiscretizerTrainBatchOp.QIndex(
						notMissingCount, quantileNum, roundType);

					for (int j = 1; j < quantileNum; ++j) {
						long index = qIndex.genIndex(j);

						if (index >= subStart && index < subEnd) {
							PairComparable pairComparable = allRows.get(
								(int) (index + localStart - subStart));
							out.collect(Tuple2.of(pairComparable.first, pairComparable.second));
						}
					}
				} else {
					QuantileDiscretizerTrainBatchOp.QIndex qIndex = new QuantileDiscretizerTrainBatchOp.QIndex(
						notMissingCount, quantileNum, roundType);

					for (int j = 1; j < quantileNum; ++j) {
						long index = qIndex.genIndex(j);
						if (index >= lessZeroCount && index < zeroCount) {
							if (subStart == 0) {
								out.collect(Tuple2.of(featureIndex, 0.0));
							}
							continue;
						}

						if (index >= lessZeroCount) {
							index = index - zeroCount;
						}

						if (index >= subStart && index < subEnd) {
							PairComparable pairComparable = allRows.get(
								(int) (index + localStart - subStart));
							out.collect(Tuple2.of(pairComparable.first, pairComparable.second));
						}
					}
				}

				localStart += subEnd - subStart;
			}
		}

		private long notMissingCount(int featureIndex) {
			int index = Collections.binarySearch(
				missingCounts, Tuple2.of(featureIndex, 0L), comparing(o -> o.f0)
			);

			if (index >= 0) {
				return totalCounts - missingCounts.get(index).f1;
			} else {
				return totalCounts;
			}
		}

		private long nonzeroCount(int featureIndex) {
			int index = Collections.binarySearch(
				nonzeroCounts, Tuple2.of(featureIndex, 0L), comparing(o -> o.f0)
			);

			if (index >= 0) {
				return nonzeroCounts.get(index).f1;
			} else {
				return 0;
			}
		}

		private long zeroCount(int featureIndex) {
			int index = Collections.binarySearch(
				nonzeroCounts, Tuple2.of(featureIndex, 0L), comparing(o -> o.f0)
			);

			if (index >= 0) {
				return totalCounts - nonzeroCounts.get(index).f1;
			} else {
				return totalCounts;
			}
		}

		private long lessZeroCount(int featureIndex) {
			int index = Collections.binarySearch(
				lessZeroCounts, Tuple2.of(featureIndex, 0L), comparing(o -> o.f0)
			);

			if (index >= 0) {
				return lessZeroCounts.get(index).f1;
			} else {
				return 0;
			}
		}

		private long featureOffset(int featureIndex) {
			int index = Collections.binarySearch(
				nonzeroOffsets, Tuple2.of(featureIndex, 0L), comparing(o -> o.f0)
			);

			if (index >= 0) {
				return nonzeroOffsets.get(index).f1;
			} else {
				return nonzeroOffsets.get(-index - 2).f1;
			}
		}

		private int featureIndexInOffsets(long offset) {
			int index = Collections.binarySearch(
				nonzeroOffsets, Tuple2.of(0, offset), comparing(o -> o.f1)
			);

			if (index >= 0) {
				return nonzeroOffsets.get(index).f0;
			} else {
				return nonzeroOffsets.get(-index - 2).f0;
			}
		}

	}
}
