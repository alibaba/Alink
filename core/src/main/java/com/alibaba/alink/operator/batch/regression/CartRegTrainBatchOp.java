package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.params.regression.CartRegTrainParams;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaltAs10;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Fit a cart regression model.
 */
@NameCn("CART决策树回归训练")
@NameEn("Cart Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.CartReg")
public class CartRegTrainBatchOp extends BaseRandomForestTrainBatchOp <CartRegTrainBatchOp> implements
	CartRegTrainParams <CartRegTrainBatchOp>,
	WithModelInfoBatchOp <TreeModelInfo.DecisionTreeModelInfo, CartRegTrainBatchOp, CartRegModelInfoBatchOp> {

	private static final long serialVersionUID = 6089717513445856264L;

	public CartRegTrainBatchOp() {
		this(null);
	}

	public CartRegTrainBatchOp(Params parameter) {
		super(parameter);

		getParams().set(TreeUtil.TREE_TYPE, TreeUtil.TreeType.MSE);
		getParams().set(HasNumTreesDefaltAs10.NUM_TREES, 1);
		getParams().set(HasFeatureSubsamplingRatio.FEATURE_SUBSAMPLING_RATIO, 1.0);
		getParams().set(HasSubsamplingRatio.SUBSAMPLING_RATIO, 1.0);
	}

	@Override
	public CartRegModelInfoBatchOp getModelInfoBatchOp() {
		return new CartRegModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
