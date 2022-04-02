package com.alibaba.alink.operator.batch.audio;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.audio.ExtractMfccFeatureMapper;
import com.alibaba.alink.params.audio.ExtractMfccFeatureParams;

@NameCn("MFCC特征提取")
public class ExtractMfccFeatureBatchOp extends MapBatchOp <ExtractMfccFeatureBatchOp> implements
	ExtractMfccFeatureParams<ExtractMfccFeatureBatchOp> {
	public ExtractMfccFeatureBatchOp() {
		this(null);
	}

	public ExtractMfccFeatureBatchOp(Params params) {
		super(ExtractMfccFeatureMapper::new, params);
	}
}
