package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.regression.AFTModelMapper;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Accelerated Failure Time Survival Regression.
 * Based on the Weibull distribution of the survival time.
 * <p>
 * (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)
 */
@NameCn("生存回归模型")
public class AftSurvivalRegressionModel extends MapModel <AftSurvivalRegressionModel> {

	private static final long serialVersionUID = 8070360321519687291L;

	public AftSurvivalRegressionModel() {this(null);}

	public AftSurvivalRegressionModel(Params params) {
		super(AFTModelMapper::new, params);
	}

}
