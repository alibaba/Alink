package com.alibaba.alink.operator.common.tree.paralleltree;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.classification.RandomForestTrainParams;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.tree.HasTreeType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.tree.paralleltree.TreeObj.NUM_OF_SUBTASKS;
import static com.alibaba.alink.operator.common.tree.paralleltree.TreeObj.N_LOCAL_ROW;
import static com.alibaba.alink.operator.common.tree.paralleltree.TreeObj.TASK_ID;

public class TreeInitObj extends ComputeFunction {
	private Params params;

	public TreeInitObj(Params params) {
		this.params = params;
	}

	private static QuantileDiscretizerModelDataConverter initialMapping(List <Row> quantileModel) {
		if (!quantileModel.isEmpty()) {
			QuantileDiscretizerModelDataConverter quantileDiscretizerModel = new QuantileDiscretizerModelDataConverter();
			quantileDiscretizerModel.load(quantileModel);

			return quantileDiscretizerModel;
		} else {
			return null;
		}
	}

	@Override
	public void calc(ComContext context) {
		if (context.getStepNo() != 1) {
			return;
		}

		List <Row> dataRows = context.getObj("treeInput");
		List <Row> quantileModel = context.getObj("quantileModel");
		List <Row> stringIndexerModel = context.getObj("stringIndexerModel");
		List<Object[]> labels = context.getObj("labels");

		int nLocalRow = dataRows == null ? 0 : dataRows.size();

		Params localParams = params.clone();
		localParams.set(TASK_ID, context.getTaskId());
		localParams.set(NUM_OF_SUBTASKS, context.getNumTask());
		localParams.set(N_LOCAL_ROW, nLocalRow);

		QuantileDiscretizerModelDataConverter quantileDiscretizerModel = initialMapping(quantileModel);

		List<String> lookUpColNames = new ArrayList<>();

		if (params.get(HasCategoricalCols.CATEGORICAL_COLS) != null) {
			lookUpColNames.addAll(Arrays.asList(params.get(HasCategoricalCols.CATEGORICAL_COLS)));
		}

		Map<String, Integer> categoricalColsSize = TreeUtil.extractCategoricalColsSize(
			stringIndexerModel, lookUpColNames.toArray(new String[0]));

		if (!Criteria.isRegression(params.get(HasTreeType.TREE_TYPE))) {
			categoricalColsSize.put(params.get(HasLabelCol.LABEL_COL), labels.get(0).length);
		}

		FeatureMeta[] featureMetas = TreeUtil.getFeatureMeta(
			params.get(HasFeatureCols.FEATURE_COLS),
			categoricalColsSize
		);

		FeatureMeta labelMeta = TreeUtil.getLabelMeta(
			params.get(HasLabelCol.LABEL_COL),
			params.get(HasFeatureCols.FEATURE_COLS).length,
			categoricalColsSize);

		TreeObj treeObj;

		if (Criteria.isRegression(params.get(HasTreeType.TREE_TYPE))) {
			treeObj = new RegObj(localParams, quantileDiscretizerModel, featureMetas, labelMeta);
		} else {
			treeObj = new ClassifierObj(localParams, quantileDiscretizerModel, featureMetas, labelMeta);
		}

		int nFeatureCol = localParams.get(RandomForestTrainParams.FEATURE_COLS).length;

		int[] data = new int[nFeatureCol * nLocalRow];

		double[] regLabels = null;
		int[] classifyLabels = null;

		if (Criteria.isRegression(params.get(HasTreeType.TREE_TYPE))) {
			regLabels = new double[nLocalRow];
		} else {
			classifyLabels = new int[nLocalRow];
		}

		int agg = 0;
		for (int iter = 0; iter < nLocalRow; ++iter) {

			for (int i = 0; i < nFeatureCol; ++i) {
				data[i * nLocalRow + agg] = (int) dataRows.get(iter).getField(i);
			}

			if (Criteria.isRegression(params.get(HasTreeType.TREE_TYPE))) {
				regLabels[agg] = (double) dataRows.get(iter).getField(nFeatureCol);
			} else {
				classifyLabels[agg] = (int) dataRows.get(iter).getField(nFeatureCol);
			}

			agg++;
		}

		treeObj.setFeatures(data);

		if (Criteria.isRegression(params.get(HasTreeType.TREE_TYPE))) {
			treeObj.setLabels(regLabels);
		} else {
			treeObj.setLabels(classifyLabels);
		}

		double[] histBuffer = new double[treeObj.getMaxHistBufferSize()];
		context.putObj("allReduce", histBuffer);
		treeObj.setHist(histBuffer);

		treeObj.initialRoot();

		context.putObj("treeObj", treeObj);
	}

}
