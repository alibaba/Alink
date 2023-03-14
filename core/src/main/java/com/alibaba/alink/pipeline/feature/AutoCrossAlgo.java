package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.feature.AutoCrossPredictParams;
import com.alibaba.alink.params.feature.AutoCrossTrainParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("")
public class AutoCrossAlgo extends Trainer <AutoCrossAlgo, AutoCrossAlgoModel> implements
	AutoCrossTrainParams <AutoCrossAlgo>,
	AutoCrossPredictParams <AutoCrossAlgo> {

	private static final long serialVersionUID = -9047554421047801735L;

	public AutoCrossAlgo() {
	}

	public AutoCrossAlgo(Params params) {
		super(params);
	}

}
