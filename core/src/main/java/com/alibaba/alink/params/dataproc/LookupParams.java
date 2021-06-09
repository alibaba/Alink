package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface LookupParams<T> extends
	ModelMapperParams <T>,
	HasSelectedCols <T>,
	HasReservedColsDefaultAsNull <T>,
	HasOutputColsDefaultAsNull <T> {

	ParamInfo <String[]> MAP_KEY_COLS = ParamInfoFactory.createParamInfo("mapKeyCols", String[].class)
		.setDescription("the names of the key column in map data table.")
		.setHasDefaultValue(null)
		.build();

	default String[] getMapKeyCols() {
		return get(MAP_KEY_COLS);
	}

	default T setMapKeyCols(String... value) {
		return set(MAP_KEY_COLS, value);
	}

	ParamInfo <String[]> MAP_VALUE_COLS = ParamInfoFactory.createParamInfo("mapValueCols", String[].class)
		.setDescription("the names of the value column in map data table.")
		.setHasDefaultValue(null)
		.build();

	default String[] getMapValueCols() {
		return get(MAP_VALUE_COLS);
	}

	default T setMapValueCols(String... value) {
		return set(MAP_VALUE_COLS, value);
	}

	ParamInfo <ModelStreamUpdateMethod> MODEL_STREAM_UPDATE_METHOD
		= ParamInfoFactory.createParamInfo("modelStreamUpdateMethod", ModelStreamUpdateMethod.class)
		.setDescription("method of model stream update.")
		.setHasDefaultValue(ModelStreamUpdateMethod.COMPLETE)
		.build();

	default ModelStreamUpdateMethod getModelStreamUpdateMethod() {
		return get(MODEL_STREAM_UPDATE_METHOD);
	}

	default T setModelStreamUpdateMethod(String value) {
		return set(MODEL_STREAM_UPDATE_METHOD, ParamUtil.searchEnum(MODEL_STREAM_UPDATE_METHOD, value));
	}

	default T setModelStreamUpdateMethod(ModelStreamUpdateMethod value) {
		return set(MODEL_STREAM_UPDATE_METHOD, value);
	}

	enum ModelStreamUpdateMethod {
		COMPLETE,
		INCREMENT
	}

}
