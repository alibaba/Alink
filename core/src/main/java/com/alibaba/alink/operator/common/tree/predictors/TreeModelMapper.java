package com.alibaba.alink.operator.common.tree.predictors;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelMapper;
import com.alibaba.alink.operator.common.dataproc.NumericalTypeCastMapper;
import com.alibaba.alink.operator.common.tree.LabelCounter;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.operator.common.tree.TreeModelDataConverter;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.dataproc.MultiStringIndexerPredictParams;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.commons.lang3.ArrayUtils;

import java.util.List;

public abstract class TreeModelMapper extends RichModelMapper {
	private static final long serialVersionUID = 9011361290985109124L;
	protected TreeModelDataConverter treeModel = new TreeModelDataConverter();

	protected int[] featuresIndex;
	protected int featureSize;

	protected MultiStringIndexerModelMapper stringIndexerModelPredictor;
	protected NumericalTypeCastMapper stringIndexerModelNumericalTypeCastMapper;
	protected NumericalTypeCastMapper numericalTypeCastMapper;

	protected int[] stringIndexerModelPredictorInputIndex;
	protected int[] stringIndexerModelPredictorOutputIndex;
	protected int[] stringIndexerModelNumericalTypeCastMapperInputIndex;
	protected int[] stringIndexerModelNumericalTypeCastMapperOutputIndex;
	protected int[] numericalTypeCastMapperInputIndex;
	protected int[] numericalTypeCastMapperOutputIndex;

	protected boolean zeroAsMissing;

	public TreeModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	private void initRowPredict() {

		final TableSchema dataSchema = getDataSchema();
		final TableSchema modelSchema = getModelSchema();

		String[] categoricalColNames = null;

		if (treeModel.meta.contains(HasCategoricalCols.CATEGORICAL_COLS)) {
			categoricalColNames = treeModel.meta.get(HasCategoricalCols.CATEGORICAL_COLS);
		}

		if (treeModel.stringIndexerModelSerialized != null) {

			final Params stringIndexerModelPredictorParams = new Params()
				.set(HasSelectedCols.SELECTED_COLS, categoricalColNames)
				.set(MultiStringIndexerPredictParams.HANDLE_INVALID, HasHandleInvalid.HandleInvalid.SKIP);

			stringIndexerModelPredictor = new MultiStringIndexerModelMapper(
				modelSchema, dataSchema, stringIndexerModelPredictorParams
			);

			stringIndexerModelPredictor.loadModel(treeModel.stringIndexerModelSerialized);

			stringIndexerModelPredictorInputIndex = TableUtil.findColIndicesWithAssertAndHint(
				dataSchema, stringIndexerModelPredictorParams.get(HasSelectedCols.SELECTED_COLS)
			);

			stringIndexerModelPredictorOutputIndex = TableUtil.findColIndicesWithAssertAndHint(
				dataSchema, stringIndexerModelPredictor.getResultCols()
			);

			final Params stringIndexerModelNumericalTypeCastMapperParams = new Params()
				.set(NumericalTypeCastParams.SELECTED_COLS, categoricalColNames)
				.set(NumericalTypeCastParams.TARGET_TYPE, NumericalTypeCastParams.TargetType.valueOf("INT"));

			stringIndexerModelNumericalTypeCastMapper = new NumericalTypeCastMapper(getDataSchema(),
				stringIndexerModelNumericalTypeCastMapperParams
			);

			stringIndexerModelNumericalTypeCastMapperInputIndex = TableUtil.findColIndicesWithAssertAndHint(
				dataSchema,
				stringIndexerModelNumericalTypeCastMapperParams.get(NumericalTypeCastParams.SELECTED_COLS)
			);

			stringIndexerModelNumericalTypeCastMapperOutputIndex = TableUtil.findColIndicesWithAssertAndHint(
				dataSchema,
				stringIndexerModelNumericalTypeCastMapper.getResultCols()
			);
		}

		final Params numericalTypeCastMapperParams = new Params()
			.set(
				NumericalTypeCastParams.SELECTED_COLS,
				ArrayUtils.removeElements(treeModel.meta.get(HasFeatureCols.FEATURE_COLS), categoricalColNames)
			)
			.set(NumericalTypeCastParams.TARGET_TYPE, NumericalTypeCastParams.TargetType.valueOf("DOUBLE"));

		numericalTypeCastMapper = new NumericalTypeCastMapper(
			dataSchema, numericalTypeCastMapperParams
		);

		numericalTypeCastMapperInputIndex = TableUtil.findColIndicesWithAssertAndHint(
			dataSchema, numericalTypeCastMapperParams.get(NumericalTypeCastParams.SELECTED_COLS)
		);

		numericalTypeCastMapperOutputIndex = TableUtil.findColIndicesWithAssertAndHint(
			dataSchema,
			numericalTypeCastMapper.getResultCols()
		);

		initFeatureIndices(treeModel.meta, dataSchema);
	}

	protected void init(List <Row> modelRows) {
		treeModel.load(modelRows);

		zeroAsMissing = treeModel.meta.get(Preprocessing.ZERO_AS_MISSING);

		if (Preprocessing.isSparse(params)) {
			return;
		}

		initRowPredict();
	}

	protected void transform(Row row) throws Exception {
		if (stringIndexerModelPredictor != null) {
			stringIndexerModelPredictor.bufferMap(
				row,
				stringIndexerModelNumericalTypeCastMapperInputIndex,
				stringIndexerModelNumericalTypeCastMapperOutputIndex
			);

			stringIndexerModelNumericalTypeCastMapper.bufferMap(
				row,
				stringIndexerModelNumericalTypeCastMapperInputIndex,
				stringIndexerModelNumericalTypeCastMapperOutputIndex
			);
		}

		numericalTypeCastMapper.bufferMap(
			row,
			numericalTypeCastMapperInputIndex,
			numericalTypeCastMapperOutputIndex
		);
	}

	private void processMissingByWeightConfidence(Row row, Node node, LabelCounter result, double weight)
		throws Exception {
		int subSize = node.getNextNodes().length;
		double[] curWeights = new double[subSize];
		double curSum = 0.;

		for (int i = 0; i < subSize; ++i) {
			double curWeight = node.getNextNodes()[i].getCounter().getWeightSum();
			curWeights[i] = curWeight;
			curSum += curWeight;
		}

		if (curSum == 0) {
			throw new Exception("Model is broken. Sum weight is zero.");
		}

		for (int i = 0; i < subSize; ++i) {
			curWeights[i] /= curSum;
		}

		for (int i = 0; i < subSize; ++i) {
			predict(row, node.getNextNodes()[i], result, weight * curWeights[i]);
		}
	}

	private void processMissingByMissingSplit(Row row, Node node, LabelCounter result, double weight) throws
		Exception {
		int[] missingSplit = node.getMissingSplit();

		int subSize = missingSplit.length;
		double[] curWeights = new double[subSize];
		double curSum = 0.;

		for (int i = 0; i < subSize; ++i) {
			double curWeight = node.getNextNodes()[missingSplit[i]].getCounter().getWeightSum();
			curWeights[i] = curWeight;
			curSum += curWeight;
		}

		if (curSum == 0) {
			throw new Exception("Model is broken. Sum weight is zero.");
		}

		for (int i = 0; i < subSize; ++i) {
			curWeights[i] /= curSum;
		}

		for (int i = 0; i < subSize; ++i) {
			predict(row, node.getNextNodes()[missingSplit[i]], result, weight * curWeights[i]);
		}
	}

	public void predict(Row row, Node node, LabelCounter result, double weight) throws Exception {
		if (node.isLeaf()) {
			result.add(node.getCounter(), weight);
			return;
		}

		int featureIndex = node.getFeatureIndex();

		int predictionFeatureIndex = featuresIndex[featureIndex];

		if (predictionFeatureIndex < 0) {
			throw new Exception("Can not find train column index: " + featureIndex);
		}

		Object featureValue = row.getField(predictionFeatureIndex);

		int[] categoryHash = node.getCategoricalSplit();

		if (Preprocessing.isMissing(featureValue, categoryHash == null, zeroAsMissing)) {
			if (node.getMissingSplit() != null) {
				processMissingByMissingSplit(row, node, result, weight);
			} else {
				processMissingByWeightConfidence(row, node, result, weight);
			}
		} else {
			if (categoryHash == null) {
				if ((Double) featureValue <= node.getContinuousSplit()) {
					//left
					predict(row, node.getNextNodes()[0], result, weight);
				} else {
					//right
					predict(row, node.getNextNodes()[1], result, weight);
				}
			} else {
				int hashValue = categoryHash[(int) featureValue];
				if (hashValue < 0) {
					processMissingByWeightConfidence(row, node, result, weight);
				} else {
					predict(row, node.getNextNodes()[hashValue], result, weight);
				}
			}
		}
	}

	private void initFeatureIndices(Params meta, TableSchema dataSchema) {
		String[] featureNames = meta.get(HasFeatureCols.FEATURE_COLS);
		this.featureSize = featureNames.length;
		this.featuresIndex = new int[this.featureSize];
		for (int i = 0; i < featureSize; i++) {
			featuresIndex[i] = TableUtil.findColIndex(dataSchema.getFieldNames(), featureNames[i]);
		}
	}
}
