package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.regression.RandomForestRegPredictParams;
import com.alibaba.alink.params.regression.RandomForestRegTrainParams;
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
@NameCn("随机森林回归")
public class RandomForestRegressor extends Trainer <RandomForestRegressor, RandomForestRegressionModel>
	implements RandomForestRegTrainParams <RandomForestRegressor>,
	RandomForestRegPredictParams <RandomForestRegressor>,
	HasLazyPrintModelInfo <RandomForestRegressor> {

	private static final long serialVersionUID = -546571785160719333L;

	public RandomForestRegressor() {
		super();
	}

	public RandomForestRegressor(Params params) {
		super(params);
	}

}
