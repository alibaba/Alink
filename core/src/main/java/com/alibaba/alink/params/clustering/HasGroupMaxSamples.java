package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasGroupMaxSamples<T> extends WithParams <T> {

	@NameCn("每个分组的最大样本数")
	@DescCn("每个分组的最大样本数")
	ParamInfo <Integer> GROUP_MAX_SAMPLES = ParamInfoFactory
		.createParamInfo("groupMaxSamples", Integer.class)
		.setDescription("the max samples in a group")
		.setHasDefaultValue(Integer.MAX_VALUE)
		.setAlias(new String[] {"groupMaxSample"})
		.build();

	default Integer getGroupMaxSamples() {return get(GROUP_MAX_SAMPLES);}

	default T setGroupMaxSamples(Integer value) {return set(GROUP_MAX_SAMPLES, value);}
}
