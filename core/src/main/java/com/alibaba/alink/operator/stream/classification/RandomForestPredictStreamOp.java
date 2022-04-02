package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.RandomForestPredictParams;

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
@NameCn("随机森林预测")
public final class RandomForestPredictStreamOp extends ModelMapStreamOp <RandomForestPredictStreamOp>
	implements RandomForestPredictParams <RandomForestPredictStreamOp> {
	private static final long serialVersionUID = 7003225554629532400L;

	public RandomForestPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public RandomForestPredictStreamOp(BatchOperator model, Params params) {
		super(model, RandomForestModelMapper::new, params);
	}
}
