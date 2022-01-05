package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.linalg.VectorType;
import com.alibaba.alink.params.ParamUtil;

public interface HasVectorType<T> extends WithParams<T> {

	/**
	 * @cn-name 要转换的Vector类型。
	 * @cn 要转换的Vector类型。
	 */
	ParamInfo <VectorType> VECTOR_TYPE = ParamInfoFactory
		.createParamInfo("vectorType", VectorType.class)
		.setDescription("Vector type is sparse or not.")
		.setOptional()
		.setHasDefaultValue(null)
		.build();

	default VectorType getVectorType() {
		return get(VECTOR_TYPE);
	}

	default T setVectorType(VectorType type) {
		return set(VECTOR_TYPE, type);
	}

	default T setVectorType(String type) {
		return set(VECTOR_TYPE, ParamUtil.searchEnum(VECTOR_TYPE, type));
	}
}
