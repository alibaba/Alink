package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

/**
 * Quantile Num.
 */
public interface HasQuantileNum<T> extends WithParams <T> {

	@NameCn("分位个数")
	@DescCn("分位个数")
	ParamInfo <Integer> QUANTILE_NUM = ParamInfoFactory
		.createParamInfo("quantileNum", Integer.class)
		.setDescription("quantile num")
		.setValidator(new MinValidator <>(0))
		.setRequired()
		.setAlias(new String[] {"N"})
		.build();

	default Integer getQuantileNum() {
		return get(QUANTILE_NUM);
	}

	default T setQuantileNum(Integer value) {
		return set(QUANTILE_NUM, value);
	}
}
