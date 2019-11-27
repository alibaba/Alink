package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.FeatureHasherMapper;
import com.alibaba.alink.params.feature.FeatureHasherParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Projects a number of categorical or numerical features into a feature vector of a specified dimension.
 *
 * (https://en.wikipedia.org/wiki/Feature_hashing)
 */
public class FeatureHasher extends MapTransformer<FeatureHasher>
	implements FeatureHasherParams <FeatureHasher> {

	public FeatureHasher() {
		this(new Params());
	}

	public FeatureHasher(Params params) {
		super(FeatureHasherMapper::new, params);
	}

}
