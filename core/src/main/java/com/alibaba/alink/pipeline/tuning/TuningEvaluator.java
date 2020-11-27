package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.batch.BatchOperator;

public abstract class TuningEvaluator<T extends TuningEvaluator <T>>
	implements WithParams <T> {

	protected Params params;

	TuningEvaluator(Params params) {
		if (null == params) {
			this.params = new Params();
		} else {
			this.params = params.clone();
		}
	}

	@Override
	public Params getParams() {
		if (null == this.params) {
			this.params = new Params();
		}

		return params;
	}

	public abstract double evaluate(BatchOperator <?> in);

	public abstract boolean isLargerBetter();

	abstract ParamInfo <Double> getMetricParamInfo();
}
