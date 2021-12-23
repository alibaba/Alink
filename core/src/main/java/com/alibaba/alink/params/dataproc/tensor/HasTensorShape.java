package com.alibaba.alink.params.dataproc.tensor;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import java.util.Arrays;

public interface HasTensorShape<T> extends WithParams <T> {

	/**
	 * @cn-name 张量形状
	 * @cn 张量的形状，数组类型。
	 */
	ParamInfo <Long[]> TENSOR_SHAPE = ParamInfoFactory
		.createParamInfo("tensorShape", Long[].class)
		.setOptional()
		.setHasDefaultValue(null)
		.build();

	default Long[] getTensorShape() {
		return get(TENSOR_SHAPE);
	}

	default T setTensorShape(Long... tensorShape) {
		return set(TENSOR_SHAPE, tensorShape);
	}

	default T setTensorShape(Integer... tensorShape) {
		return setTensorShape(Arrays.stream(tensorShape).map(Long::valueOf).toArray(Long[]::new));
	}
}
