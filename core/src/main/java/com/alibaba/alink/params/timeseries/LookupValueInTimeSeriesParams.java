package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.feature.featuregenerator.HasTimeCol;
import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface LookupValueInTimeSeriesParams<T> extends
	MapperParams <T>,
	HasTimeCol <T>,
	HasOutputCol <T>,
	HasReservedColsDefaultAsNull <T> {

	/**
	 * @cn-name 时间序列列
	 * @cn 时间序列列，是特殊的MTable类型，一列是时间，一列是值
	 */
	ParamInfo <String> TIME_SERIES_COL = ParamInfoFactory
		.createParamInfo("timeSeriesCol", String.class)
		.setDescription("the time series column")
		.setRequired()
		.build();

	default String getTimeSeriesCol() {
		return get(TIME_SERIES_COL);
	}

	default T setTimeSeriesCol(String value) {
		return set(TIME_SERIES_COL, value);
	}
}
