package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSamplingMethod<T> extends WithParams <T> {

	@NameCn("起始点列名")
	@DescCn("用来指定起始点列")
	ParamInfo <String> SAMPLING_METHOD = ParamInfoFactory
		.createParamInfo("samplingMethod", String.class)
		.setDescription("sampling method, e.g., ALIAS, PARTIAL_SUM")
		.setHasDefaultValue("ALIAS")
		.build();

	/**
	 * get sampling method
	 *
	 * @return
	 */
	default String getSamplingMethod() {return get(SAMPLING_METHOD);}

	/**
	 * set sampling method
	 * @param value
	 * @return
	 */
	default T setSamplingMethod(String value) {return set(SAMPLING_METHOD, value);}
}