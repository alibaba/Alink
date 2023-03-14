package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.feature.MultiHotPredictParams;
import com.alibaba.alink.params.feature.MultiHotTrainParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("多热编码")
public class MultiHotEncoder extends Trainer<MultiHotEncoder, MultiHotEncoderModel> implements
    MultiHotTrainParams<MultiHotEncoder>,
    MultiHotPredictParams<MultiHotEncoder>,
	HasLazyPrintModelInfo<MultiHotEncoder> {

	private static final long serialVersionUID = -4475238813305040400L;

	public MultiHotEncoder() {
		super();
	}

	public MultiHotEncoder(Params params) {
		super(params);
	}

}
