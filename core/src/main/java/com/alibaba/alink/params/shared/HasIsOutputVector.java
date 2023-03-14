package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * @author guotao.gt
 * @since 2019.02.20
 */
public interface HasIsOutputVector<T> extends WithParams <T> {

	@NameCn("输出是否为向量格式")
	@DescCn("输出是否为向量格式")
	ParamInfo <Boolean> IS_OUTPUT_VECTOR = ParamInfoFactory
		.createParamInfo("isOutputVector", Boolean.class)
		.setDescription("the output is vector format or not")
		.setHasDefaultValue(false)
		.setAlias(new String[] {})
		.build();

	default Boolean getIsOutputVector() {
		return get(IS_OUTPUT_VECTOR);
	}

	default T setIsOutputVector(Boolean value) {
		return set(IS_OUTPUT_VECTOR, value);
	}
}
