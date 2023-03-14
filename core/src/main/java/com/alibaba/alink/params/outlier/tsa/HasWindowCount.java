package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasWindowCount<T> extends WithParams <T> {

	@NameCn("异常检测方法")
	@DescCn("异常检测方法")
	ParamInfo <Integer> WINDOW_COUNT = ParamInfoFactory
		.createParamInfo("windowCount", Integer.class)
		.setDescription("time series function type")
		.setHasDefaultValue(5)
		.build();

	default Integer getWindowCount() {
		return get(WINDOW_COUNT);
	}

	default T setWindowCount(Integer value) {
		return set(WINDOW_COUNT, value);
	}

}
