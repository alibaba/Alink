package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface StrafiedSampleWithSizeParams<T> extends WithParams <T> {

	@NameCn("分层列")
	@DescCn("分层列")
	ParamInfo <String> STRATA_COL = ParamInfoFactory
		.createParamInfo("strataCol", String.class)
		.setDescription("strata col name.")
		.setAlias(new String[] {"strataColName"})
		.setRequired()
		.build();

	@NameCn("采样个数")
	@DescCn("采样个数")
	ParamInfo <Integer> STRATA_SIZE = ParamInfoFactory
		.createParamInfo("strataSize", Integer.class)
		.setDescription("strata size.")
		.setHasDefaultValue(-1)
		.build();

	@NameCn("采样个数")
	@DescCn("采样个数, eg, a:10,b:30")
	ParamInfo <String> STRATA_SIZES = ParamInfoFactory
		.createParamInfo("strataSizes", String.class)
		.setDescription("strata sizes. a:10,b:30")
		.setRequired()
		.build();

	@NameCn("是否放回")
	@DescCn("是否有放回的采样，默认不放回")
	ParamInfo <Boolean> WITH_REPLACEMENT = ParamInfoFactory
		.createParamInfo("withReplacement", Boolean.class)
		.setDescription("Indicates whether to enable sampling with replacement, default is without replcement")
		.setHasDefaultValue(false)
		.build();

	default String getStrataCol() {
		return get(STRATA_COL);
	}

	default T setStrataCol(String value) {
		return set(STRATA_COL, value);
	}

	default Integer getStrataSize() {
		return get(STRATA_SIZE);
	}

	default T setStrataSize(Integer value) {
		return set(STRATA_SIZE, value);
	}

	default String getStrataSizes() {
		return get(STRATA_SIZES);
	}

	default T setStrataSizes(String value) {
		return set(STRATA_SIZES, value);
	}

	default Boolean getWithReplacement() {
		return get(WITH_REPLACEMENT);
	}

	default T setWithReplacement(Boolean value) {
		return set(WITH_REPLACEMENT, value);
	}
}
