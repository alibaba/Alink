package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMinFrequencyDefaultAsMinus1<T> extends
	WithParams <T> {
	@NameCn("最低频次")
	@DescCn("最低频次，过滤掉出现次数小于该频次的特征")
	ParamInfo <Integer> MIN_FREQUENCY = ParamInfoFactory
		.createParamInfo("minFrequency", Integer.class)
		.setDescription("min frequency")
		.setHasDefaultValue(-1)
		.build();

	default Integer getMinFrequency() {
		return get(MIN_FREQUENCY);
	}

	default T setMinFrequency(Integer value) {
		return set(MIN_FREQUENCY, value);
	}
}
