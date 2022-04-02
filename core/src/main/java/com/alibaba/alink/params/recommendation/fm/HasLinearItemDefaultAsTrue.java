package com.alibaba.alink.params.recommendation.fm;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * has linear item or not.
 */
public interface HasLinearItemDefaultAsTrue<T> extends WithParams <T> {
	@NameCn("是否含有线性项")
	@DescCn("是否含有线性项")
	ParamInfo <Boolean> WITH_LINEAR_ITEM = ParamInfoFactory
		.createParamInfo("withLinearItem", Boolean.class)
		.setDescription("with linear item.")
		.setHasDefaultValue(true)
		.build();

	default Boolean getWithLinearItem() {
		return get(WITH_LINEAR_ITEM);
	}

	default T setWithLinearItem(Boolean value) {
		return set(WITH_LINEAR_ITEM, value);
	}
}
