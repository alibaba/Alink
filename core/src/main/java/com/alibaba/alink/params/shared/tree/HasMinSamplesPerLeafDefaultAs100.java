package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMinSamplesPerLeafDefaultAs100<T> extends WithParams <T> {
	/**
	 * @cn-name 叶节点的最小样本个数
	 * @cn 叶节点的最小样本个数
	 */
	ParamInfo <Integer> MIN_SAMPLES_PER_LEAF = ParamInfoFactory
		.createParamInfo("minSamplesPerLeaf", Integer.class)
		.setDescription("Minimal number of sample in one leaf.")
		.setHasDefaultValue(100)
		.setAlias(new String[] {"minLeafSample"})
		.build();

	default Integer getMinSamplesPerLeaf() {
		return get(MIN_SAMPLES_PER_LEAF);
	}

	default T setMinSamplesPerLeaf(Integer value) {
		return set(MIN_SAMPLES_PER_LEAF, value);
	}
}

