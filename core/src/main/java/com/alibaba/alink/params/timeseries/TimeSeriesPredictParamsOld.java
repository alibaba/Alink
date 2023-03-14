package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface TimeSeriesPredictParamsOld<T> extends
	MapperParams <T>,
	HasValueCol <T>,
	HasReservedColsDefaultAsNull <T>,
	HasPythonCmdPath <T> {
	@NameCn("时间列名")
	@DescCn("时间列对应的列名")
	ParamInfo <String> TIME_COL = ParamInfoFactory
		.createParamInfo("timeCol", String.class)
		.setDescription("time col name")
		.build();

	default String getTimeCol() {
		return get(TIME_COL);
	}

	default T setTimeCol(String value) {
		return set(TIME_COL, value);
	}

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

	ParamInfo <String> PREDICT_TIME_VALUE_COL = ParamInfoFactory
		.createParamInfo("predictTimeValueCol", String.class)
		.setDescription("predictTimeValueCol")
		.setRequired()
		.build();

	default String getPredictTimeValueCol() {
		return get(PREDICT_TIME_VALUE_COL);
	}

	default T setPredictTimeValueCol(String value) {
		return set(PREDICT_TIME_VALUE_COL, value);
	}

	ParamInfo <String> PREDICT_NEXT_VALUES_COL = ParamInfoFactory
		.createParamInfo("predictNextValuesCol", String.class)
		.setDescription("predictNextValuesCol")
		.setRequired()
		.build();

	default String getPredictNextValuesCol() {
		return get(PREDICT_NEXT_VALUES_COL);
	}

	default T setPredictNextValuesCol(String value) {
		return set(PREDICT_NEXT_VALUES_COL, value);
	}

	@NameCn("分组列")
	@DescCn("分组单列名，必选")
	ParamInfo <String> GROUP_COL = ParamInfoFactory
		.createParamInfo("groupCol", String.class)
		.setDescription("Name of a grouping column")
		.setAlias(new String[] {"groupColName", "groupIdCol", "groupIdColName"})
		.build();

	default String getGroupCol() {
		return get(GROUP_COL);
	}

	default T setGroupCol(String colName) {
		return set(GROUP_COL, colName);
	}

	@Deprecated
	default T setGroupIdCol(String colName) {
		return set(GROUP_COL, colName);
	}

}
