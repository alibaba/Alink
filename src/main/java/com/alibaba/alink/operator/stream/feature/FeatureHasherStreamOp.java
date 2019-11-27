package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.FeatureHasherMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.feature.FeatureHasherParams;

/**
 * Projects a number of categorical or numerical features into a feature vector of a specified dimension.
 * <p>
 * (https://en.wikipedia.org/wiki/Feature_hashing)
 */
public class FeatureHasherStreamOp extends MapStreamOp <FeatureHasherStreamOp>
	implements FeatureHasherParams <FeatureHasherStreamOp> {
	public FeatureHasherStreamOp() {
		this(null);
	}

	public FeatureHasherStreamOp(Params params) {
		super(FeatureHasherMapper::new, params);
	}
}
