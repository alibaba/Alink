package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.dataproc.tensor.HasTensorShape;
import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * Parameters of tensor to vector.
 */
public interface VectorToTensorParams<T> extends SISOMapperParams <T>, HasTensorShape <T> {
	/**
	 * @cn-name 张量数据类型
	 * @cn 张量中数据的类型。
	 */
	ParamInfo <String> TENSOR_DATA_TYPE = ParamInfoFactory
		.createParamInfo("tensorDataType", String.class)
		.setOptional()
		.setHasDefaultValue(null)
		.build();

	default String getTensorDataType() {
		return get(TENSOR_DATA_TYPE);
	}

	default T setTensorDataType(String dataType) {
		return set(TENSOR_DATA_TYPE, dataType);
	}
}