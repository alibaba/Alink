package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.regression.DecisionTreeRegPredictParams;
import com.alibaba.alink.params.regression.DecisionTreeRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

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
@NameCn("决策树回归")
public class DecisionTreeRegressor extends Trainer <DecisionTreeRegressor, DecisionTreeRegressionModel> implements
	DecisionTreeRegTrainParams <DecisionTreeRegressor>, DecisionTreeRegPredictParams <DecisionTreeRegressor>,
	HasLazyPrintModelInfo <DecisionTreeRegressor> {

	private static final long serialVersionUID = 3306625963195536351L;

	public DecisionTreeRegressor() {
		super();
	}

	public DecisionTreeRegressor(Params params) {
		super(params);
	}

}
