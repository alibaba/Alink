package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;
import com.alibaba.alink.params.classification.SoftmaxTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Softmax is a multi class classifier.
 */
@NameCn("Softmax")
public class Softmax extends Trainer <Softmax, SoftmaxModel> implements
	SoftmaxTrainParams <Softmax>, SoftmaxPredictParams <Softmax>,
	HasLazyPrintTrainInfo <Softmax>, HasLazyPrintModelInfo <Softmax> {

	private static final long serialVersionUID = -4375182004172665465L;

	public Softmax() {super();}

	public Softmax(Params params) {
		super(params);
	}

}
