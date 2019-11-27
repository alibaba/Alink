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
import com.alibaba.alink.operator.common.tree.TreeModelDataConverter;
import com.alibaba.alink.params.dataproc.MultiStringIndexerPredictParams;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;
import com.alibaba.alink.params.shared.HasHandleInvalid;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.commons.lang3.ArrayUtils;

import java.util.List;

public abstract class TreeModelMapper extends RichModelMapper {
	protected TreeModelDataConverter treeModel = new TreeModelDataConverter();

	protected int[] featuresIndex;
	protected int featureSize;

	protected MultiStringIndexerModelMapper stringIndexerModelPredictor;
	protected NumericalTypeCastMapper stringIndexerModelNumericalTypeCastMapper;
	protected NumericalTypeCastMapper numericalTypeCastMapper;

	public TreeModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	protected void init(List<Row> modelRows) {
		TableSchema dataSchema = getDataSchema();
		treeModel.load(modelRows);

		String[] categoricalColNames = null;

		if (treeModel.meta.contains(HasCategoricalCols.CATEGORICAL_COLS)) {
			categoricalColNames = treeModel.meta.get(HasCategoricalCols.CATEGORICAL_COLS);
		}

		if (treeModel.stringIndexerModelSerialized != null) {
			TableSchema modelSchema = getModelSchema();
			stringIndexerModelPredictor = new MultiStringIndexerModelMapper(
				modelSchema,
				dataSchema,
				treeModel
					.meta
					.set(HasSelectedCols.SELECTED_COLS, categoricalColNames)
					.set(MultiStringIndexerPredictParams.HANDLE_INVALID, "skip")
			);

			stringIndexerModelPredictor.loadModel(treeModel.stringIndexerModelSerialized);

			stringIndexerModelNumericalTypeCastMapper = new NumericalTypeCastMapper(dataSchema,
				new Params()
					.set(NumericalTypeCastParams.SELECTED_COLS, categoricalColNames)
					.set(NumericalTypeCastParams.TARGET_TYPE, "INT")
			);
		}

		numericalTypeCastMapper = new NumericalTypeCastMapper(dataSchema,
			new Params()
				.set(
					NumericalTypeCastParams.SELECTED_COLS,
					ArrayUtils.removeElements(treeModel.meta.get(HasFeatureCols.FEATURE_COLS), categoricalColNames)
				)
				.set(NumericalTypeCastParams.TARGET_TYPE, "DOUBLE")
		);

		initFeatureIndexes(treeModel.meta, dataSchema);
	}

	protected Row transRow(Row row) throws Exception {
		if (stringIndexerModelPredictor != null) {
			row = stringIndexerModelPredictor.map(row);
			row = stringIndexerModelNumericalTypeCastMapper.map(row);
		}

		return numericalTypeCastMapper.map(row);
	}

	private void ProcessMissing(Row row, Node node, LabelCounter result, double weight) throws Exception {
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
			Predict(row, node.getNextNodes()[i], result, weight * curWeights[i]);
		}
	}

	public void Predict(Row row, Node node, LabelCounter result, double weight) throws Exception {
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

		if (featureValue == null) {
			ProcessMissing(row, node, result, weight);
		} else {
			int[] categoryHash = node.getCategoricalSplit();

			if (categoryHash == null) {
				if ((Double) featureValue <= node.getContinuousSplit()) {
					//left
					Predict(row, node.getNextNodes()[0], result, weight);
				} else {
					//right
					Predict(row, node.getNextNodes()[1], result, weight);
				}
			} else {
				int hashValue = categoryHash[(int) featureValue];
				if (hashValue < 0) {
					ProcessMissing(row, node, result, weight);
				} else {
					Predict(row, node.getNextNodes()[hashValue], result, weight);
				}
			}
		}
	}

	private void initFeatureIndexes(Params meta, TableSchema dataSchema) {
		String[] featureNames = meta.get(HasFeatureCols.FEATURE_COLS);
		this.featureSize = featureNames.length;
		this.featuresIndex = new int[this.featureSize];
		for (int i = 0; i < featureSize; i++) {
			featuresIndex[i] = TableUtil.findColIndex(dataSchema.getFieldNames(), featureNames[i]);
		}
	}
}
