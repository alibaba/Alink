package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.classification.C45TrainParams;
import com.alibaba.alink.params.feature.C45EncoderParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("C45编码")
public class C45Encoder extends Trainer <C45Encoder, C45EncoderModel> implements
	C45TrainParams <C45Encoder>,
	C45EncoderParams <C45Encoder> {

	private static final long serialVersionUID = -8593371277511217184L;

	public C45Encoder() {
	}

	public C45Encoder(Params params) {
		super(params);
	}

}
