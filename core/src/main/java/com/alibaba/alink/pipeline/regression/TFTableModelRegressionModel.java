package com.alibaba.alink.pipeline.regression;

import org.apache.flink.annotation.Internal;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.regression.tensorflow.TFTableModelRegressionModelMapper;
import com.alibaba.alink.params.regression.TFTableModelRegressionPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@Internal
public class TFTableModelRegressionModel<T extends TFTableModelRegressionModel <T>> extends MapModel <T>
	implements TFTableModelRegressionPredictParams <T> {

	public TFTableModelRegressionModel() {this(null);}

	public TFTableModelRegressionModel(Params params) {
		super(TFTableModelRegressionModelMapper::new, params);
	}
}
