package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxSampleNumPerGroup<T> extends WithParams <T> {
	@NameCn("每组最大样本数目")
	@DescCn("每组最大样本数目")
	ParamInfo <Integer> MAX_SAMPLE_NUM_PER_GROUP = ParamInfoFactory
		.createParamInfo("maxSampleNumPerGroup", Integer.class)
		.setDescription("the maximum number of samples per group.")
		.build();

	default Integer getMaxSampleNumPerGroup() {
		return get(MAX_SAMPLE_NUM_PER_GROUP);
	}

	default T setMaxSampleNumPerGroup(Integer value) {
		return set(MAX_SAMPLE_NUM_PER_GROUP, value);
	}
}
