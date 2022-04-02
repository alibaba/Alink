package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasKvColDelimiterDefaultAsComma<T> extends WithParams <T> {
	@NameCn("分隔符")
	@DescCn("当输入数据为稀疏格式时，key-value对之间的分隔符")
	ParamInfo <String> KV_COL_DELIMITER = ParamInfoFactory
		.createParamInfo("kvColDelimiter", String.class)
		.setDescription("Delimiter used between key-value pairs when data in the input table is in sparse format")
		.setAlias(new String[] {"colDelimiter"})
		.setHasDefaultValue(",")
		.build();

	default String getKvColDelimiter() {
		return get(KV_COL_DELIMITER);
	}

	default T setKvColDelimiter(String value) {
		return set(KV_COL_DELIMITER, value);
	}
}
