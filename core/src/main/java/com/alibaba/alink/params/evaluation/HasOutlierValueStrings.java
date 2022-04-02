package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Outlier values in string format.
 */
public interface HasOutlierValueStrings<T> extends WithParams <T> {
	@NameCn("异常值")
	@DescCn("异常值，需要是字符串格式")
	ParamInfo <String[]> OUTLIER_VALUE_STRINGS = ParamInfoFactory
		.createParamInfo("outlierValueStrings", String[].class)
		.setDescription("Outlier values in string format.")
		.build();

	default String[] getOutlierValueStrings() {
		return get(OUTLIER_VALUE_STRINGS);
	}

	default T setOutlierValueStrings(String... value) {
		return set(OUTLIER_VALUE_STRINGS, value);
	}
}
