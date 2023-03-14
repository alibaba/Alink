package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface TSPredictFuncTypeParams<T> extends WithParams <T> {
	@NameCn("时间序列方法")
	@DescCn("时间序列方法")
	ParamInfo <TimeSeriesPredictType> TIME_SERIES_FUNC_TYPE = ParamInfoFactory
		.createParamInfo("timeSeriesFuncType", TimeSeriesPredictType.class)
		.setDescription("time series function type")
		.setHasDefaultValue(TimeSeriesPredictType.Arima)
		.build();

	default TimeSeriesPredictType getTimeSeriesFuncType() {
		return get(TIME_SERIES_FUNC_TYPE);
	}

	default T setTimeSeriesFuncType(TimeSeriesPredictType value) {
		return set(TIME_SERIES_FUNC_TYPE, value);
	}

	default T setTimeSeriesFuncType(String value) {
		return set(TIME_SERIES_FUNC_TYPE, ParamUtil.searchEnum(TIME_SERIES_FUNC_TYPE, value));
	}

	enum TimeSeriesPredictType {
		HoltWinters,
		Arima,
		Garch,
		ArimaGarch
	}
}
