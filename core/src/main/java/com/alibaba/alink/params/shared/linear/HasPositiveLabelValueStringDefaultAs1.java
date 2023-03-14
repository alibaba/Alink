package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * positive label value with string format.
 */
public interface HasPositiveLabelValueStringDefaultAs1<T> extends WithParams <T> {
	@NameCn("正样本")
	@DescCn("正样本对应的字符串格式。")
	ParamInfo <String> POS_LABEL_VAL_STR = ParamInfoFactory
		.createParamInfo("positiveLabelValueString", String.class)
		.setDescription("positive label value with string format.")
		.setHasDefaultValue("1")
		.setAlias(new String[] {"predPositiveLabelValueString", "positiveValue"})
		.build();

	default String getPositiveLabelValueString() {
		return get(POS_LABEL_VAL_STR);
	}

	default T setPositiveLabelValueString(String value) {
		return set(POS_LABEL_VAL_STR, value);
	}
}
