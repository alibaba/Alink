package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Fit a random forest classification model.
 *
 * @see BaseRandomForestTrainBatchOp
 */
@NameCn("随机森林训练")
@NameEn("Random Forest Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.RandomForestClassifier")
public class RandomForestTrainBatchOp extends BaseRandomForestTrainBatchOp <RandomForestTrainBatchOp> implements
	RandomForestTrainParams <RandomForestTrainBatchOp>,
	WithModelInfoBatchOp <TreeModelInfo.RandomForestModelInfo, RandomForestTrainBatchOp, RandomForestModelInfoBatchOp> {

	private static final long serialVersionUID = 608820075935409265L;

	public RandomForestTrainBatchOp() {
		this(null);
	}

	public RandomForestTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public RandomForestModelInfoBatchOp getModelInfoBatchOp() {
		return new RandomForestModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
