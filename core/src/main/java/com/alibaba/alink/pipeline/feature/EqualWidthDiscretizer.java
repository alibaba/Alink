package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.params.feature.QuantileDiscretizerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * EqualWidth discretizer keeps every interval the same width, output the interval
 * as model, and can transform a new data using the model.
 * <p>The output is the index of the interval.
 */
@NameCn("等宽离散化")
public class EqualWidthDiscretizer extends Trainer <EqualWidthDiscretizer, QuantileDiscretizerModel>
	implements QuantileDiscretizerTrainParams <EqualWidthDiscretizer>,
	QuantileDiscretizerPredictParams <EqualWidthDiscretizer>, HasLazyPrintModelInfo <EqualWidthDiscretizer> {
	private static final long serialVersionUID = 5516757585031745048L;

	public EqualWidthDiscretizer() {
		super();
	}

	public EqualWidthDiscretizer(Params params) {
		super(params);
	}

}
