package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasNumTreesOfInfoGain<T> extends WithParams <T> {
	@NameCn("模型中Id3树的棵数")
	@DescCn("模型中Id3树的棵数")
	ParamInfo <Integer> NUM_TREES_OF_INFO_GAIN = ParamInfoFactory
		.createParamInfo("numTreesOfInfoGain", Integer.class)
		.setDescription("Number of id3 trees.")
		.setHasDefaultValue(null)
		.build();

	default Integer getNumTreesOfInfoGain() {
		return get(NUM_TREES_OF_INFO_GAIN);
	}

	default T setNumTreesOfInfoGain(Integer value) {
		return set(NUM_TREES_OF_INFO_GAIN, value);
	}
}
