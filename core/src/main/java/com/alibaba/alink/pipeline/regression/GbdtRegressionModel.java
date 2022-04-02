package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import com.alibaba.alink.params.regression.GbdtRegPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * The model of gbdt regression.
 */
@NameCn("GBDT回归模型")
public class GbdtRegressionModel extends MapModel <GbdtRegressionModel>
	implements GbdtRegPredictParams <GbdtRegressionModel> {

	private static final long serialVersionUID = 7672795584627981972L;

	public GbdtRegressionModel() {
		this(null);
	}

	public GbdtRegressionModel(Params params) {
		super(GbdtModelMapper::new, params);
	}

}
