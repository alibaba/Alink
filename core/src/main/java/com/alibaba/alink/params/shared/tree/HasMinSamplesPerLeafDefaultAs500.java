package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMinSamplesPerLeafDefaultAs500<T> extends WithParams <T> {
	@NameCn("叶节点的最小样本个数")
	@DescCn("叶节点的最小样本个数")
	ParamInfo <Integer> MIN_SAMPLES_PER_LEAF = ParamInfoFactory
		.createParamInfo("minSamplesPerLeaf", Integer.class)
		.setDescription("Minimal number of sample in one leaf.")
		.setHasDefaultValue(500)
		.setAlias(new String[] {"minLeafSample"})
		.build();

	default Integer getMinSamplesPerLeaf() {
		return get(MIN_SAMPLES_PER_LEAF);
	}

	default T setMinSamplesPerLeaf(Integer value) {
		return set(MIN_SAMPLES_PER_LEAF, value);
	}
}

