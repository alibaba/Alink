package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface RedisStringSinkParams <T> extends
	RedisParams <T>{

	@NameCn("单键列")
	@DescCn("单键列")
	ParamInfo <String> KEY_COL = ParamInfoFactory
		.createParamInfo("keyCol", String.class)
		.setDescription("key column name")
		.setHasDefaultValue(null)
		.build();
	
	@NameCn("单值列")
	@DescCn("单值列")
	ParamInfo <String> VALUE_COL = ParamInfoFactory
		.createParamInfo("valueCol", String.class)
		.setDescription("value column name")
		.setHasDefaultValue(null)
		.build();

	default String getValueCol() {return get(VALUE_COL);}
	default T setValueCol(String value) {return set(VALUE_COL, value);}
	
	default String getKeyCol() {return get(KEY_COL);}
	default T setKeyCol(String value) {return set(KEY_COL, value);}
	
}
