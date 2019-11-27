package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.mapper.MISOMapperParams;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * parameters of vector assembler.
 *
 */
public interface VectorAssemblerParams<T> extends
	MISOMapperParams<T> {

	ParamInfo <String> HANDLE_INVALID = ParamInfoFactory
		.createParamInfo("handleInvalid", String.class)
		.setDescription("parameter for how to handle invalid data (NULL values)")
		.setHasDefaultValue("error")
		.build();

	/**
	 * parameter how to handle invalid data (NULL values). Options are 'skip' (filter out rows with
	 * invalid data), 'error' (throw an error), or 'keep' (return relevant number of NaN in the output).
	 */
	default String getHandleInvalid() {return get(HANDLE_INVALID);}

	default T setHandleInvalid(String value) {return set(HANDLE_INVALID, value);}
}
