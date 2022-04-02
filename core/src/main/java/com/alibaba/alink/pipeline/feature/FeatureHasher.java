package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.FeatureHasherMapper;
import com.alibaba.alink.params.feature.FeatureHasherParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Projects a number of categorical or numerical features into a feature vector of a specified dimension.
 *
 * (https://en.wikipedia.org/wiki/Feature_hashing)
 */
@NameCn("特征哈希")
public class FeatureHasher extends MapTransformer <FeatureHasher>
	implements FeatureHasherParams <FeatureHasher> {

	private static final long serialVersionUID = -2116325139103333804L;

	public FeatureHasher() {
		this(new Params());
	}

	public FeatureHasher(Params params) {
		super(FeatureHasherMapper::new, params);
	}

}
