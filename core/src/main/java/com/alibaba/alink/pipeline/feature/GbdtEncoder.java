package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import com.alibaba.alink.params.feature.GbdtEncoderParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("GBDT编码")
public class GbdtEncoder extends Trainer <GbdtEncoder, GbdtEncoderModel> implements
	GbdtTrainParams <GbdtEncoder>,
	GbdtEncoderParams <GbdtEncoder> {

	private static final long serialVersionUID = -6668055009648285896L;

	public GbdtEncoder() {
		this(new Params());
	}

	public GbdtEncoder(Params params) {
		super(params);
	}

}
