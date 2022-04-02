package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

@ParamSelectColumnSpec(name = "valueCol", allowedTypeCollections = TypeCollections.MTABLE_TYPES)
public interface TimeSeriesPredictParams<T> extends
	MapperParams <T>,
	HasValueCol <T>,
	HasPredictionCol <T>,
	HasPredictionDetailCol <T>,
	HasReservedColsDefaultAsNull <T> {

	@NameCn("预测条数")
	@DescCn("预测条数")
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
