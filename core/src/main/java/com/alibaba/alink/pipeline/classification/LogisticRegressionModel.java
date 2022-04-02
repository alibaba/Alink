package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.classification.LogisticRegressionPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Logistic regression pipeline model.
 */
@NameCn("逻辑回归模型")
public class LogisticRegressionModel extends MapModel <LogisticRegressionModel>
	implements LogisticRegressionPredictParams <LogisticRegressionModel> {

	private static final long serialVersionUID = -667086258847413637L;

	public LogisticRegressionModel() {this(null);}

	public LogisticRegressionModel(Params params) {
		super(LinearModelMapper::new, params);
	}

}
