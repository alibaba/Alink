package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasOdds<T> extends WithParams <T> {
	@NameCn("分数基准点处的odds值")
	@DescCn("分数基准点处的odds值")
	ParamInfo <Double> ODDS = ParamInfoFactory
		.createParamInfo("odds", Double.class)
		.setDescription("odds")
		.setHasDefaultValue(null)
		.build();

	default Double getOdds() {
		return get(ODDS);
	}

	default T setOdds(Double value) {
		return set(ODDS, value);
	}
}
