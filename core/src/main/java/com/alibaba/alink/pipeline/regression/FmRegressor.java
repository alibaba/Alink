package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.params.recommendation.FmPredictParams;
import com.alibaba.alink.params.recommendation.FmTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Fm regression pipeline op.
 */
@NameCn("FM回归")
public class FmRegressor extends Trainer <FmRegressor, FmRegressionModel>
	implements FmTrainParams <FmRegressor>, FmPredictParams <FmRegressor>, HasLazyPrintModelInfo <FmRegressor>,
	HasLazyPrintTrainInfo <FmRegressor> {

	private static final long serialVersionUID = 3075669888794004056L;

	public FmRegressor() {
		super();
	}

	public FmRegressor(Params params) {
		super(params);
	}

}
