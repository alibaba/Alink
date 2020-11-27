package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.regression.DecisionTreeRegPredictParams;

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
public final class DecisionTreeRegPredictBatchOp extends ModelMapBatchOp <DecisionTreeRegPredictBatchOp> implements
	DecisionTreeRegPredictParams <DecisionTreeRegPredictBatchOp> {
	private static final long serialVersionUID = -8850643703068190492L;

	public DecisionTreeRegPredictBatchOp() {
		this(null);
	}

	public DecisionTreeRegPredictBatchOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}
}
