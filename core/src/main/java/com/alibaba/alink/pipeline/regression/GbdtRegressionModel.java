package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.GbdtRegPredictParams;
import com.alibaba.alink.pipeline.MapModel;

public class GbdtRegressionModel extends MapModel<GbdtRegressionModel>
	implements GbdtRegPredictParams <GbdtRegressionModel> {

	public GbdtRegressionModel() {
		this(null);
	}

	public GbdtRegressionModel(Params params) {
		super(GbdtModelMapper::new, params);
	}

}