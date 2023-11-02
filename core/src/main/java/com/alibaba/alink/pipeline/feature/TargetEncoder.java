package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.params.feature.TargetEncoderPredictParams;
import com.alibaba.alink.params.feature.TargetEncoderTrainParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("目标编码")
@NameEn("Target Encoder")
public class TargetEncoder extends Trainer <TargetEncoder, TargetEncoderModel>
	implements TargetEncoderTrainParams <TargetEncoder>,
	TargetEncoderPredictParams <TargetEncoder> {

	public TargetEncoder() {
		this(new Params());
	}

	public TargetEncoder(Params params) {
		super(params);
	}

}
