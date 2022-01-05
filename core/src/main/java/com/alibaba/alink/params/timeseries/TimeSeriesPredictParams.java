package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface TimeSeriesPredictParams<T> extends
	MapperParams <T>,
	HasValueCol <T>,
	HasPredictionCol <T>,
	HasPredictionDetailCol <T>,
	HasReservedColsDefaultAsNull <T> {

	/**
	 * @cn-name 预测条数
	 * @cn 预测条数
	 */
	ParamInfo <Integer> PREDICT_NUM = ParamInfoFactory
		.createParamInfo("predictNum", Integer.class)
		.setDescription("the predict num")
		.setHasDefaultValue(1)
		.build();

	default Integer getPredictNum() {
		return get(PREDICT_NUM);
	}

	default T setPredictNum(Integer value) {
		return set(PREDICT_NUM, value);
	}

}
