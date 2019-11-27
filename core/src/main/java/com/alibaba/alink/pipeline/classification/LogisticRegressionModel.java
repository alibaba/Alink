package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.LogisticRegressionPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Logistic regression pipeline model.
 *
 */
public class LogisticRegressionModel extends MapModel<LogisticRegressionModel>
	implements LogisticRegressionPredictParams <LogisticRegressionModel> {

	public LogisticRegressionModel() {this(null);}

	public LogisticRegressionModel(Params params) {
		super(LinearModelMapper::new, params);
	}

}
