package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.feature.AutoCrossPredictParams;
import com.alibaba.alink.params.feature.AutoCrossTrainParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("Auto Cross")
public class AutoCross extends Trainer <AutoCross, AutoCrossModel> implements
	AutoCrossTrainParams <AutoCross>,
	AutoCrossPredictParams <AutoCross> {

	private static final long serialVersionUID = -4475238813305040400L;

	public AutoCross() {
	}

	public AutoCross(Params params) {
		super(params);
	}

}
