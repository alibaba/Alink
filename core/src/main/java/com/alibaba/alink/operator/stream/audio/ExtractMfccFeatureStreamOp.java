package com.alibaba.alink.operator.stream.audio;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.audio.ExtractMfccFeatureMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.audio.ExtractMfccFeatureParams;

@NameCn("MFCC特征提取")
@NameEn("MFCC Feature Extract")
public class ExtractMfccFeatureStreamOp extends MapStreamOp <ExtractMfccFeatureStreamOp> implements
	ExtractMfccFeatureParams <ExtractMfccFeatureStreamOp> {
	public ExtractMfccFeatureStreamOp() {
		this(null);
	}

	public ExtractMfccFeatureStreamOp(Params params) {
		super(ExtractMfccFeatureMapper::new, params);
	}
}
