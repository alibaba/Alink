package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
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
@NameCn("决策树回归预测")
@NameEn("Decision Tree Regresion Predict")
public final class DecisionTreeRegPredictStreamOp extends ModelMapStreamOp <DecisionTreeRegPredictStreamOp>
	implements DecisionTreeRegPredictParams <DecisionTreeRegPredictStreamOp> {
	private static final long serialVersionUID = 1886411464321075840L;

	public DecisionTreeRegPredictStreamOp() {
		super(RandomForestModelMapper::new, new Params());
	}

	public DecisionTreeRegPredictStreamOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}

	public DecisionTreeRegPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public DecisionTreeRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, RandomForestModelMapper::new, params);
	}
}
