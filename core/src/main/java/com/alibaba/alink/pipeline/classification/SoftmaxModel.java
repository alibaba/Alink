package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.linear.SoftmaxModelMapper;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Softmax pipeline model.
 */
@NameCn("Softmax模型")
public class SoftmaxModel extends MapModel <SoftmaxModel>
	implements SoftmaxPredictParams <SoftmaxModel> {

	private static final long serialVersionUID = -1082618513481687945L;

	public SoftmaxModel() {this(null);}

	public SoftmaxModel(Params params) {
		super(SoftmaxModelMapper::new, params);
	}

}
