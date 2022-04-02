package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Trait for parameter withStd. If true, Scales the data to unit standard deviation.
 */
public interface HasWithStd<T> extends WithParams <T> {

	@NameCn("是否使用标准差")
	@DescCn("是否使用标准差，默认使用")
	ParamInfo <Boolean> WITH_STD = ParamInfoFactory
		.createParamInfo("withStd", Boolean.class)
		.setDescription("Scales the data to unit standard deviation. true by default")
		.setHasDefaultValue(true)
		.build();

	default Boolean getWithStd() {
		return get(WITH_STD);
	}

	default T setWithStd(Boolean value) {
		return set(WITH_STD, value);
	}
}
