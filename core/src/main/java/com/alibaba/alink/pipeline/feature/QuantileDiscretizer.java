package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.params.feature.QuantileDiscretizerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Quantile discretizer calculate the q-quantile as the interval, output the interval
 * as model, and can transform a new data using the model.
 * <p>The output is the index of the interval.
 */
@NameCn("分位数离散化")
public class QuantileDiscretizer extends Trainer <QuantileDiscretizer, QuantileDiscretizerModel>
	implements QuantileDiscretizerTrainParams <QuantileDiscretizer>,
	QuantileDiscretizerPredictParams <QuantileDiscretizer>,
	HasLazyPrintModelInfo <QuantileDiscretizer> {
	private static final long serialVersionUID = -8169259273624463843L;

	public QuantileDiscretizer() {
		super();
	}

	public QuantileDiscretizer(Params params) {
		super(params);
	}

}
