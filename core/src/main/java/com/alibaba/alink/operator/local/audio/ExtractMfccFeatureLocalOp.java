package com.alibaba.alink.operator.local.audio;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.audio.ExtractMfccFeatureMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.audio.ExtractMfccFeatureParams;

@NameCn("MFCC特征提取")
public class ExtractMfccFeatureLocalOp extends MapLocalOp <ExtractMfccFeatureLocalOp> implements
	ExtractMfccFeatureParams <ExtractMfccFeatureLocalOp> {
	public ExtractMfccFeatureLocalOp() {
		this(null);
	}

	public ExtractMfccFeatureLocalOp(Params params) {
		super(ExtractMfccFeatureMapper::new, params);
	}
}
