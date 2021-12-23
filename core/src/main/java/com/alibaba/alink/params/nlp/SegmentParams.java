package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * Params for segment.
 */
public interface SegmentParams<T> extends SISOMapperParams <T> {

	/**
	 * @cn-name 用户自定义字典
	 * @cn 用户自定义字典
	 */
	ParamInfo <String[]> USER_DEFINED_DICT = ParamInfoFactory
		.createParamInfo("userDefinedDict", String[].class)
		.setDescription("User defined dict for segment.")
		.setHasDefaultValue(null)
		.build();

	default String[] getUserDefinedDict() {
		return this.getParams().get(USER_DEFINED_DICT);
	}

	default T setUserDefinedDict(String... value) {
		return set(USER_DEFINED_DICT, value);
	}

}
