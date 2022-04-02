package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.params.regression.RandomForestRegTrainParams;

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
@NameCn("随机森林回归训练")
public final class RandomForestRegTrainBatchOp extends BaseRandomForestTrainBatchOp <RandomForestRegTrainBatchOp>
	implements RandomForestRegTrainParams <RandomForestRegTrainBatchOp>,
	WithModelInfoBatchOp <TreeModelInfo.RandomForestModelInfo, RandomForestRegTrainBatchOp,
		RandomForestRegModelInfoBatchOp> {

	private static final long serialVersionUID = 4812293019930417017L;

	public RandomForestRegTrainBatchOp() {
		this(null);
	}

	public RandomForestRegTrainBatchOp(Params params) {
		super(params);
		this.getParams().set(TreeUtil.TREE_TYPE, TreeUtil.TreeType.MSE);
	}

	@Override
	public RandomForestRegModelInfoBatchOp getModelInfoBatchOp() {
		return new RandomForestRegModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
