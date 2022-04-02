package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.params.classification.CartTrainParams;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaltAs10;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;

/**
 * Fit a cart model.
 */
@NameCn("CART决策树分类训练")
public final class CartTrainBatchOp extends BaseRandomForestTrainBatchOp <CartTrainBatchOp>
	implements CartTrainParams <CartTrainBatchOp>,
	WithModelInfoBatchOp <TreeModelInfo.DecisionTreeModelInfo, CartTrainBatchOp, CartModelInfoBatchOp> {

	private static final long serialVersionUID = -224441106863805421L;

	public CartTrainBatchOp() {
		this(null);
	}

	public CartTrainBatchOp(Params parameter) {
		super(parameter);

		getParams().set(TreeUtil.TREE_TYPE, TreeUtil.TreeType.GINI);
		getParams().set(HasNumTreesDefaltAs10.NUM_TREES, 1);
		getParams().set(HasFeatureSubsamplingRatio.FEATURE_SUBSAMPLING_RATIO, 1.0);
		getParams().set(HasSubsamplingRatio.SUBSAMPLING_RATIO, 1.0);
	}

	@Override
	public CartModelInfoBatchOp getModelInfoBatchOp() {
		return new CartModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
