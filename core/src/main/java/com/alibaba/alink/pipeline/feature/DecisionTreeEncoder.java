package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.classification.DecisionTreeTrainParams;
import com.alibaba.alink.params.feature.DecisionTreeEncoderParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("决策树编码")
public class DecisionTreeEncoder extends Trainer <DecisionTreeEncoder, DecisionTreeEncoderModel> implements
	DecisionTreeTrainParams <DecisionTreeEncoder>,
	DecisionTreeEncoderParams <DecisionTreeEncoder> {

	private static final long serialVersionUID = -8593371277511217184L;

	public DecisionTreeEncoder() {
	}

	public DecisionTreeEncoder(Params params) {
		super(params);
	}

}
