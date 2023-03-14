package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.classification.DecisionTreeTrainParams;
import com.alibaba.alink.params.feature.DecisionTreeRegEncoderParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("决策树回归编码")
public class DecisionTreeRegEncoder extends Trainer <DecisionTreeRegEncoder, DecisionTreeRegEncoderModel> implements
	DecisionTreeTrainParams <DecisionTreeRegEncoder>,
	DecisionTreeRegEncoderParams <DecisionTreeRegEncoder> {

	private static final long serialVersionUID = -8593371277511217184L;

	public DecisionTreeRegEncoder() {
	}

	public DecisionTreeRegEncoder(Params params) {
		super(params);
	}

}
