package com.alibaba.alink.operator.common.tree.parallelcart.data;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.feature.ContinuousRanges;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp;
import com.alibaba.alink.operator.common.tree.parallelcart.BuildLocalSketch;
import com.alibaba.alink.operator.common.tree.parallelcart.EpsilonApproQuantile;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class DataUtil {

	public static Data createData(
		Params params, List <FeatureMeta> featureMetas, int m, boolean useSort) {

		featureMetas.sort(Comparator.comparingInt(FeatureMeta::getIndex));

		int column = 0;
		int maxColumnIndex = -1;
		for (FeatureMeta meta : featureMetas) {
			Preconditions.checkState(
				meta.getIndex() == column++,
				"There are empty columns. index: %d",
				meta.getIndex()
			);
			maxColumnIndex = Math.max(maxColumnIndex, meta.getIndex());
		}
		maxColumnIndex += 1;

		if (Preprocessing.isSparse(params)) {
			return new SparseData(
				params, featureMetas.toArray(new FeatureMeta[0]), m, maxColumnIndex
			);
		} else {
			return new DenseData(
				params, featureMetas.toArray(new FeatureMeta[0]), m, maxColumnIndex, useSort
			);
		}
	}

	public static boolean left(int val, Node node, FeatureMeta featureMeta) {
		if (node.getMissingSplit() != null && Preprocessing.isMissing(val, featureMeta.getMissingIndex())) {
			return node.getMissingSplit()[0] == 0;
		}
		if (node.getCategoricalSplit() == null) {
			return val <= node.getContinuousSplit();
		} else {
			return node.getCategoricalSplit()[val] == 0;
		}
	}

	public static boolean leftUseSummary(
		double val, Node node, EpsilonApproQuantile.WQSummary summary, FeatureMeta featureMeta, boolean
		zeroAsMissing) {

		if (node.getMissingSplit() != null
			&& Preprocessing.isMissing(val, featureMeta, zeroAsMissing)) {
			return node.getMissingSplit()[0] == 0;
		}

		return val <= summary.entries.get((int) node.getContinuousSplit()).value;
	}

	public static int getFeatureCategoricalSize(FeatureMeta featureMeta, boolean useMissing) {
		return useMissing ? featureMeta.getNumCategorical() + 1 : featureMeta.getNumCategorical();
	}

	public static DataSet <FeatureMeta> createContinuousMetaFromQuantile(
		final DataSet <Row> quantileModel, final Params params) {

		return quantileModel.reduceGroup(new GroupReduceFunction <Row, FeatureMeta>() {
			private static final long serialVersionUID = 2798937980102249481L;

			@Override
			public void reduce(Iterable <Row> values, Collector <FeatureMeta> out) throws Exception {
				ArrayList <Row> quantileRows = new ArrayList <>();

				for (Row row : values) {
					quantileRows.add(row);
				}

				QuantileDiscretizerModelDataConverter quantileModel
					= new QuantileDiscretizerModelDataConverter();

				quantileModel.load(quantileRows);

				if (Preprocessing.isSparse(params)) {
					for (Map.Entry <String, ContinuousRanges> entry : quantileModel.data.entrySet()) {
						out.collect(
							new FeatureMeta(
								entry.getKey(),
								Integer.parseInt(entry.getKey()),
								FeatureMeta.FeatureType.CONTINUOUS,
								quantileModel.getFeatureSize(entry.getKey()),
								Preprocessing.zeroIndex(quantileModel, entry.getKey()),
								quantileModel.missingIndex(entry.getKey())
							)
						);
					}
				} else {
					for (Map.Entry <String, ContinuousRanges> entry : quantileModel.data.entrySet()) {
						out.collect(
							new FeatureMeta(
								entry.getKey(),
								TableUtil.findColIndex(params.get(HasFeatureCols.FEATURE_COLS), entry.getKey()),
								FeatureMeta.FeatureType.CONTINUOUS,
								quantileModel.getFeatureSize(entry.getKey()),
								-1,
								quantileModel.missingIndex(entry.getKey())
							)
						);
					}
				}
			}
		});
	}

	public static DataSet <FeatureMeta> createCategoricalMetaFromStringIndexer(
		final DataSet <Row> stringIndexerModel, final Params params) {

		return stringIndexerModel
			.reduceGroup(new GroupReduceFunction <Row, FeatureMeta>() {
				private static final long serialVersionUID = 8374084070326048116L;

				@Override
				public void reduce(Iterable <Row> values, Collector <FeatureMeta> out) throws Exception {
					ArrayList <Row> stringIndexerRows = new ArrayList <>();

					for (Row row : values) {
						stringIndexerRows.add(row);
					}

					MultiStringIndexerModelData multiStringIndexerModel
						= new MultiStringIndexerModelDataConverter().load(stringIndexerRows);

					List <String> lookUpCols = new ArrayList <>();

					if (params.contains(HasCategoricalCols.CATEGORICAL_COLS)
						&& params.get(HasCategoricalCols.CATEGORICAL_COLS) != null) {
						lookUpCols.addAll(Arrays.asList(params.get(HasCategoricalCols.CATEGORICAL_COLS)));
					}

					Map <String, Integer> categoricalColsSize = TreeUtil.extractCategoricalColsSize(
						multiStringIndexerModel, lookUpCols.toArray(new String[0]));

					for (Map.Entry <String, Integer> entry : categoricalColsSize.entrySet()) {
						out.collect(
							new FeatureMeta(
								entry.getKey(),
								TableUtil.findColIndex(params.get(HasFeatureCols.FEATURE_COLS), entry.getKey()),
								FeatureMeta.FeatureType.CATEGORICAL,
								entry.getValue(),
								// unsupported now.
								-1,
								entry.getValue()
							)
						);
					}
				}
			});

	}

	public static DataSet <FeatureMeta> createFeatureMetas(
		final DataSet <Row> quantileModel, final DataSet <Row> stringIndexerModel, final Params params) {

		return createCategoricalMetaFromStringIndexer(stringIndexerModel, params)
			.union(createContinuousMetaFromQuantile(quantileModel, params));
	}

	public static DataSet <FeatureMeta> createOneHotFeatureMeta(
		DataSet <Row> trainDataSet, Params params, String[] trainColNames) {

		return maxIndexOfVector(trainDataSet, params, trainColNames)
			.flatMap(new FlatMapFunction <Tuple1 <Integer>, FeatureMeta>() {
				private static final long serialVersionUID = -3553369535923306216L;

				@Override
				public void flatMap(Tuple1 <Integer> value, Collector <FeatureMeta> out) throws Exception {
					for (int i = 0; i < value.f0; ++i) {
						out.collect(
							new FeatureMeta(String.valueOf(i), i, FeatureMeta.FeatureType.CONTINUOUS, 2, 0, 2)
						);
					}
				}
			});
	}

	public static DataSet <FeatureMeta> createEpsilonApproQuantileFeatureMeta(
		final DataSet <Row> trainDataSet, final DataSet <Row> stringIndexerModel,
		final Params params, final String[] trainColNames, final long sessionId) {

		if (params.contains(GbdtTrainParams.VECTOR_COL)) {
			return maxIndexOfVector(trainDataSet, params, trainColNames)
				.flatMap(new FlatMapFunction <Tuple1 <Integer>, FeatureMeta>() {
					private static final long serialVersionUID = 2747512134712849290L;

					@Override
					public void flatMap(Tuple1 <Integer> value, Collector <FeatureMeta> out) {
						for (int i = 0; i < value.f0; ++i) {
							out.collect(
								new FeatureMeta(String.valueOf(i), i,
									FeatureMeta.FeatureType.CONTINUOUS,
									BuildLocalSketch.maxSize(
										params.get(BaseGbdtTrainBatchOp.SKETCH_RATIO),
										params.get(BaseGbdtTrainBatchOp.SKETCH_EPS)
									),
									-1, -1)
							);
						}
					}
				});
		} else {
			DataSet <FeatureMeta> featureMetas = DataUtil
				.createCategoricalMetaFromStringIndexer(stringIndexerModel, params);

			String[] continuousColNames = ArrayUtils.removeElements(
				params.get(HasFeatureCols.FEATURE_COLS),
				params.get(HasCategoricalCols.CATEGORICAL_COLS)
			);

			return featureMetas.union(
				MLEnvironmentFactory.get(sessionId)
					.getExecutionEnvironment()
					.fromElements(continuousColNames)
					.map(new MapFunction <String, FeatureMeta>() {
						private static final long serialVersionUID = -5825333642221837526L;

						@Override
						public FeatureMeta map(String value) {
							return new FeatureMeta(
								value,
								TableUtil.findColIndex(params.get(HasFeatureCols.FEATURE_COLS), value),
								FeatureMeta.FeatureType.CONTINUOUS,
								BuildLocalSketch.maxSize(
									params.get(BaseGbdtTrainBatchOp.SKETCH_RATIO),
									params.get(BaseGbdtTrainBatchOp.SKETCH_EPS)
								),
								-1, -1
							);
						}
					})
			);
		}
	}

	public static DataSet <Tuple1 <Integer>> maxIndexOfVector(
		DataSet <Row> trainDataSet, Params params, String[] trainColNames) {

		final int vectorColIndex = TableUtil.findColIndex(trainColNames, params.get(GbdtTrainParams.VECTOR_COL));

		return trainDataSet
			.mapPartition(new MapPartitionFunction <Row, Tuple1 <Integer>>() {
				private static final long serialVersionUID = 704094286836681792L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Tuple1 <Integer>> out) throws Exception {
					int max = -1;

					for (Row val : values) {
						Vector vector = VectorUtil.getVector(val.getField(vectorColIndex));

						if (vector instanceof SparseVector && vector.size() < 0) {
							for (int v : ((SparseVector) vector).getIndices()) {
								max = Math.max(max, v + 1);
							}
						} else {
							max = Math.max(max, vector.size());
						}
					}

					out.collect(Tuple1.of(max));
				}
			})
			.max(0);
	}
}
