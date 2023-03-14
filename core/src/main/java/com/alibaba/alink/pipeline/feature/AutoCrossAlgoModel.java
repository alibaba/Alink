package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.AutoCross.AutoCrossAlgoModelMapper;
import com.alibaba.alink.params.feature.AutoCrossPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("")
public class AutoCrossAlgoModel extends MapModel <AutoCrossAlgoModel>
	implements AutoCrossPredictParams <AutoCrossAlgoModel> {

	private static final long serialVersionUID = -749788686029860226L;

	public AutoCrossAlgoModel() {
		this(new Params());
	}

	public AutoCrossAlgoModel(Params params) {
		super(AutoCrossAlgoModelMapper::new, params);
	}
}
