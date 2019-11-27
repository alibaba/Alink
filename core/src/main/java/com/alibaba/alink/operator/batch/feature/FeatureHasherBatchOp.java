package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.FeatureHasherMapper;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.params.feature.FeatureHasherParams;

/**
 * Projects a number of categorical or numerical features into a feature vector of a specified dimension.
 *
 * (https://en.wikipedia.org/wiki/Feature_hashing)
 */
public final class FeatureHasherBatchOp extends MapBatchOp <FeatureHasherBatchOp>
	implements FeatureHasherParams <FeatureHasherBatchOp> {
	public FeatureHasherBatchOp() {
		this(null);
	}

	public FeatureHasherBatchOp(Params params) {
		super(FeatureHasherMapper::new, params);
	}
}
