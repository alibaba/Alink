package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSchemaStr;

public interface LookupRecentDaysParams<T> extends
	ModelMapperParams <T>,
	HasSelectedCols <T>,
	HasReservedColsDefaultAsNull <T>,
	HasOutputColsDefaultAsNull <T> {

	@NameCn("Schema")
	@DescCn("Schema。格式为\"colname coltype[, colname2, coltype2[, ...]]\"，例如 \"f0 string, f1 bigint, f2 double\"")
	ParamInfo <String> FEATURE_SCHEMA_STR = ParamInfoFactory
		.createParamInfo("featureSchemaStr", String.class)
		.setDescription("Formatted schema for features")
		.build();

	default String getFeatureSchemaStr() {
		return get(FEATURE_SCHEMA_STR);
	}

	default T setFeatureSchemaStr(String value) {
		return set(FEATURE_SCHEMA_STR, value);
	}

	//@NameCn("Key列名")
	//@DescCn("模型中对应的查找等值的列名")
	//ParamInfo <String[]> MAP_KEY_COLS = ParamInfoFactory.createParamInfo("mapKeyCols", String[].class)
	//	.setDescription("the names of the key column in map data table.")
	//	.setHasDefaultValue(null)
	//	.build();
	//
	//default String[] getMapKeyCols() {
	//	return get(MAP_KEY_COLS);
	//}
	//
	//default T setMapKeyCols(String... value) {
	//	return set(MAP_KEY_COLS, value);
	//}
	//
	//@NameCn("Values列名")
	//@DescCn("模型中需要拼接到样本中的列名")
	//ParamInfo <String[]> MAP_VALUE_COLS = ParamInfoFactory.createParamInfo("mapValueCols", String[].class)
	//	.setDescription("the names of the value column in map data table.")
	//	.setHasDefaultValue(null)
	//	.build();
	//
	//default String[] getMapValueCols() {
	//	return get(MAP_VALUE_COLS);
	//}
	//
	//default T setMapValueCols(String... value) {
	//	return set(MAP_VALUE_COLS, value);
	//}

	@NameCn("模型更新方法")
	@DescCn("模型更新方法，可选COMPLETE（全量更新）或者 INCREMENT（增量更新）")
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
