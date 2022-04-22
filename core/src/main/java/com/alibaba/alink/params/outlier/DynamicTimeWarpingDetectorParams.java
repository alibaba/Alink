package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface DynamicTimeWarpingDetectorParams<T> extends
	OutlierDetectorParams <T>,
	WithUniVarParams <T> {

	@NameCn("周期")
	@DescCn("周期")
	ParamInfo <Integer> PERIOD = ParamInfoFactory
		.createParamInfo("period", Integer.class)
		.setDescription("The period.")
		.setHasDefaultValue(1)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getPeriod() {
		return get(PERIOD);
	}

	default T setPeriod(Integer value) {
		return set(PERIOD, value);
	}

	@NameCn("序列长度")
	@DescCn("序列长度")
	ParamInfo <Integer> SERIES_LENGTH = ParamInfoFactory
		.createParamInfo("seriesLength", Integer.class)
		.setDescription("the length of series")
		.setHasDefaultValue(1)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getSeriesLength() {
		return get(SERIES_LENGTH);
	}

	default T setSeriesLength(Integer value) {
		return set(SERIES_LENGTH, value);
	}

	@NameCn("搜索窗口长度")
	@DescCn("搜错窗口长度")
	ParamInfo <Integer> SEARCH_WINDOW = ParamInfoFactory
		.createParamInfo("searchWindow", Integer.class)
		.setDescription("the length of search window")
		.setHasDefaultValue(1)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getSearchWindow() {
		return get(SEARCH_WINDOW);
	}

	default T setSearchWindow(Integer value) {
		return set(SEARCH_WINDOW, value);
	}

	@NameCn("历史序列个数")
	@DescCn("历史序列个数")
	ParamInfo <Integer> HISTORICAL_SERIES_NUM = ParamInfoFactory
		.createParamInfo("historicalSeriesNum", Integer.class)
		.setDescription("the Number of historical series")
		.setHasDefaultValue(1)
		.setValidator(new MinValidator <>(2))
		.build();

	default Integer getHistoricalSeriesNum() {
		return get(HISTORICAL_SERIES_NUM);
	}

	default T setHistoricalSeriesNum(Integer value) {
		return set(HISTORICAL_SERIES_NUM, value);
	}

}
