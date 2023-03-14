package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.feature.OneHotPredictParams;
import com.alibaba.alink.params.feature.OneHotTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * One hot pipeline op.
 */
@NameCn("独热编码")
public class OneHotEncoder extends Trainer <OneHotEncoder, OneHotEncoderModel> implements
	OneHotTrainParams <OneHotEncoder>,
	OneHotPredictParams <OneHotEncoder>,
	HasLazyPrintModelInfo <OneHotEncoder> {

	private static final long serialVersionUID = -987476648422234074L;

	public OneHotEncoder() {
		super();
	}

	public OneHotEncoder(Params params) {
		super(params);
	}

}
