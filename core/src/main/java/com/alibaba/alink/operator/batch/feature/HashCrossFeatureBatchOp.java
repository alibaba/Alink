package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.feature.HashCrossFeatureMapper;
import com.alibaba.alink.params.feature.HashCrossFeatureParams;

public class HashCrossFeatureBatchOp extends MapBatchOp<HashCrossFeatureBatchOp>
	implements HashCrossFeatureParams <HashCrossFeatureBatchOp> {

	public HashCrossFeatureBatchOp() {
		this(new Params());
	}

	public HashCrossFeatureBatchOp(Params params) {
		super(HashCrossFeatureMapper::new, params);
	}
}
