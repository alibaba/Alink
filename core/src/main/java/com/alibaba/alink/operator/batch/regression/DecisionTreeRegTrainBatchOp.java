package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.params.regression.DecisionTreeRegTrainParams;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaltAs10;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;

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
public final class DecisionTreeRegTrainBatchOp extends BaseRandomForestTrainBatchOp <DecisionTreeRegTrainBatchOp>
	implements DecisionTreeRegTrainParams <DecisionTreeRegTrainBatchOp>,
	WithModelInfoBatchOp <TreeModelInfo.DecisionTreeModelInfo, DecisionTreeRegTrainBatchOp,
		DecisionTreeRegModelInfoBatchOp> {

	private static final long serialVersionUID = 3078456104033825160L;

	public DecisionTreeRegTrainBatchOp() {
		this(null);
	}

	public DecisionTreeRegTrainBatchOp(Params params) {
		super(params);
		getParams().set(HasNumTreesDefaltAs10.NUM_TREES, 1);
		getParams().set(TreeUtil.TREE_TYPE, TreeUtil.TreeType.MSE);
		getParams().set(HasFeatureSubsamplingRatio.FEATURE_SUBSAMPLING_RATIO, 1.0);
		getParams().set(HasSubsamplingRatio.SUBSAMPLING_RATIO, 1.0);
	}

	@Override
	public DecisionTreeRegModelInfoBatchOp getModelInfoBatchOp() {
		return new DecisionTreeRegModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
