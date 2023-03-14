package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasPdo<T> extends WithParams <T> {
	@NameCn("分数增长pdo，odds加倍")
	@DescCn("分数增长pdo，odds加倍")
	ParamInfo <Double> PDO = ParamInfoFactory
		.createParamInfo("pdo", Double.class)
		.setDescription("pdo")
		.setHasDefaultValue(null)
		.build();

	default Double getPdo() {
		return get(PDO);
	}

	default T setPdo(Double value) {
		return set(PDO, value);
	}
}
