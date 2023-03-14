package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface TSDecomposeTimeSeriesFuncType<T> extends WithParams <T> {
	@NameCn("时间序列方法")
	@DescCn("时间序列方法")
	ParamInfo <TimeSeriesFuncType> TIME_SERIES_FUNC_TYPE = ParamInfoFactory
		.createParamInfo("timeSeriesFuncType", TimeSeriesFuncType.class)
		.setDescription("time series function type")
		.setHasDefaultValue(TimeSeriesFuncType.STL)
		.build();

	default TimeSeriesFuncType getTimeSeriesFuncType() {
		return get(TIME_SERIES_FUNC_TYPE);
	}

	default T setTimeSeriesFuncType(TimeSeriesFuncType value) {
		return set(TIME_SERIES_FUNC_TYPE, value);
	}

	default T setTimeSeriesFuncType(String value) {
		return set(TIME_SERIES_FUNC_TYPE, ParamUtil.searchEnum(TIME_SERIES_FUNC_TYPE, value));
	}

	enum TimeSeriesFuncType {
		STL,
		CONVOLUTION
	}
}
