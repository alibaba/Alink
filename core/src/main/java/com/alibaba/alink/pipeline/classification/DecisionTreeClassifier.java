package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.DecisionTreeTrainBatchOp;
import com.alibaba.alink.params.classification.DecisionTreePredictParams;
import com.alibaba.alink.params.classification.DecisionTreeTrainParams;
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
public class DecisionTreeClassifier extends Trainer <DecisionTreeClassifier, DecisionTreeClassificationModel> implements
	DecisionTreeTrainParams <DecisionTreeClassifier>,
	DecisionTreePredictParams <DecisionTreeClassifier>,
	HasLazyPrintModelInfo <DecisionTreeClassifier> {

	private static final long serialVersionUID = 6832020005078299139L;

	public DecisionTreeClassifier() {
		super();
	}

	public DecisionTreeClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new DecisionTreeTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
