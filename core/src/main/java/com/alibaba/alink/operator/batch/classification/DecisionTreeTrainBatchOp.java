package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.params.classification.DecisionTreeTrainParams;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * The random forest use the bagging to prevent the overfitting.
 *
 * <p>In the operator, we implement three type of decision tree to
 * increase diversity of the forest.
 * <ul>
 * <tr>id3</tr>
 * <tr>cart</tr>
 * <tr>c4.5</tr>
 * </ul>
 * and the criteria is
 * <ul>
 * <tr>information</tr>
 * <tr>gini</tr>
 * <tr>information ratio</tr>
 * <tr>mse</tr>
 * </ul>
 *
 * @see <a href="https://en.wikipedia.org/wiki/Random_forest">Random_forest</a>
 */
@NameCn("决策树训练")
@NameEn("Decision Tree Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.DecisionTreeClassifier")
public class DecisionTreeTrainBatchOp extends BaseRandomForestTrainBatchOp <DecisionTreeTrainBatchOp>
	implements DecisionTreeTrainParams <DecisionTreeTrainBatchOp>,
	WithModelInfoBatchOp <TreeModelInfo.DecisionTreeModelInfo, DecisionTreeTrainBatchOp, DecisionTreeModelInfoBatchOp> {

	private static final long serialVersionUID = 3469143629174787731L;

	public DecisionTreeTrainBatchOp() {
		this(null);
	}

	public DecisionTreeTrainBatchOp(Params params) {
		super(params);
		getParams().set(
			TreeUtil.TREE_TYPE,
			TreeUtil.TreeType.valueOf(getParams().get(DecisionTreeTrainParams.TREE_TYPE).name())
		);
		getParams().set(RandomForestTrainParams.NUM_TREES, 1);
		getParams().set(HasFeatureSubsamplingRatio.FEATURE_SUBSAMPLING_RATIO, 1.0);
		getParams().set(HasSubsamplingRatio.SUBSAMPLING_RATIO, 1.0);
	}

	@Override
	public DecisionTreeModelInfoBatchOp getModelInfoBatchOp() {
		return new DecisionTreeModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
