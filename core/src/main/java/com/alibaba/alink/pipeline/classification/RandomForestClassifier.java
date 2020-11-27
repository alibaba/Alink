package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.params.classification.RandomForestPredictParams;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
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
public class RandomForestClassifier extends Trainer <RandomForestClassifier, RandomForestClassificationModel>
	implements RandomForestTrainParams <RandomForestClassifier>,
	RandomForestPredictParams <RandomForestClassifier>,
	HasLazyPrintModelInfo <RandomForestClassifier> {

	private static final long serialVersionUID = 3301684197036917644L;

	public RandomForestClassifier() {
		super();
	}

	public RandomForestClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new RandomForestTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
