package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.RandomForestRegPredictParams;

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
@NameCn("随机森林回归流预测")
@NameEn("Random Forests Regresion Prediction")
public final class RandomForestsRegPredictStreamOp extends ModelMapStreamOp <RandomForestsRegPredictStreamOp>
	implements RandomForestRegPredictParams <RandomForestsRegPredictStreamOp> {
	private static final long serialVersionUID = -164443409974593152L;

	public RandomForestsRegPredictStreamOp() {
		super(RandomForestModelMapper::new, new Params());
	}

	public RandomForestsRegPredictStreamOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}

	public RandomForestsRegPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public RandomForestsRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, RandomForestModelMapper::new, params);
	}
}
