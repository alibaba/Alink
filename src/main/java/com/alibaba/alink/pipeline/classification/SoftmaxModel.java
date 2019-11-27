package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.common.linear.SoftmaxModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Softmax pipeline model.
 *
 */
public class SoftmaxModel extends MapModel<SoftmaxModel>
	implements SoftmaxPredictParams <SoftmaxModel> {

	public SoftmaxModel() {this(null);}

	public SoftmaxModel(Params params) {
		super(SoftmaxModelMapper::new, params);
	}

}
