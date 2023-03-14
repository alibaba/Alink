package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.params.regression.LassoRegPredictParams;
import com.alibaba.alink.params.regression.LassoRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Lasso regression pipeline op.
 */
@NameCn("Lasso回归")
public class LassoRegression extends Trainer <LassoRegression, LassoRegressionModel> implements
	LassoRegTrainParams <LassoRegression>,
	LassoRegPredictParams <LassoRegression>,
	HasLazyPrintTrainInfo <LassoRegression>, HasLazyPrintModelInfo <LassoRegression> {

	private static final long serialVersionUID = -7444365100929167804L;

	public LassoRegression() {
		super(new Params());
	}

	public LassoRegression(Params params) {
		super(params);
	}

}
