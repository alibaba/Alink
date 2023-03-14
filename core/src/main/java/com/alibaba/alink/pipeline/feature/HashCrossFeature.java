package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.HashCrossFeatureMapper;
import com.alibaba.alink.params.feature.HashCrossFeatureParams;
import com.alibaba.alink.pipeline.MapTransformer;

@NameCn("Hash Cross特征")
public class HashCrossFeature extends MapTransformer <HashCrossFeature>
	implements HashCrossFeatureParams <HashCrossFeature> {

	private static final long serialVersionUID = -2116325136513333804L;

	public HashCrossFeature() {
		this(new Params());
	}

	public HashCrossFeature(Params params) {
		super(HashCrossFeatureMapper::new, params);
	}
}
