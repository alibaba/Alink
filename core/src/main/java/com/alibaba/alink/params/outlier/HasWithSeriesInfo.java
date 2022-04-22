package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasWithSeriesInfo<T> extends WithParams <T> {
	@NameCn("检测异常时，是否使用序列信息")
	@DescCn("检测异常时，是否使用序列信息。默认值是false，即，不使用序列信息。")
	ParamInfo <Boolean> WITH_SERIES_INFO = ParamInfoFactory
		.createParamInfo("withSeriesInfo", Boolean.class)
		.setDescription("detect with the series info or not. Default value is false.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getWithSeriesInfo() {
		return get(WITH_SERIES_INFO);
	}

	default T setWithSeriesInfo(Boolean value) {
		return set(WITH_SERIES_INFO, value);
	}

}
