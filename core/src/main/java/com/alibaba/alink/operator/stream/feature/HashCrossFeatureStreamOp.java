package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.HashCrossFeatureMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.feature.HashCrossFeatureParams;

public class HashCrossFeatureStreamOp extends MapStreamOp <HashCrossFeatureStreamOp>
	implements HashCrossFeatureParams <HashCrossFeatureStreamOp> {

	public HashCrossFeatureStreamOp() {
		this(new Params());
	}

	public HashCrossFeatureStreamOp(Params params) {
		super(HashCrossFeatureMapper::new, params);
	}
}
