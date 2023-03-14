package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.classification.DecisionTreePredictParams;

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
@NameCn("决策树预测")
@NameEn("Decision Tree Prediction")
public final class DecisionTreePredictBatchOp extends ModelMapBatchOp <DecisionTreePredictBatchOp> implements
	DecisionTreePredictParams <DecisionTreePredictBatchOp> {
	private static final long serialVersionUID = 3664269451746168314L;

	public DecisionTreePredictBatchOp() {
		this(null);
	}

	public DecisionTreePredictBatchOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}
}
