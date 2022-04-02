package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasKeyCols<T> extends WithParams <T> {
	@NameCn("多键值列")
	@DescCn("多键值列")
	ParamInfo <String[]> KEY_COLS = ParamInfoFactory
		.createParamInfo("keyCols", String[].class)
		.setDescription("key colume names")
		.setHasDefaultValue(null)
		.build();

	default String[] getKeyCols() {return get(KEY_COLS);}

	default T setKeyCols(String... value) {return set(KEY_COLS, value);}
}
