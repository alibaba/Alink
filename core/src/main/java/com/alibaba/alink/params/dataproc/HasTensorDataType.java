package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.linalg.tensor.DataType;
import com.alibaba.alink.params.ParamUtil;

public interface HasTensorDataType<T> extends WithParams <T> {

	/**
	 * @cn-name 要转换的张量数据类型
	 * @cn 要转换的张量数据类型。
	 */
	ParamInfo <DataType> TENSOR_DATA_TYPE = ParamInfoFactory
		.createParamInfo("tensorDataType", DataType.class)
		.setDescription("Tensor data type to convert.")
		.setOptional()
		.build();

	default DataType getTensorDataType() {
		return get(TENSOR_DATA_TYPE);
	}

	default T setTensorDataType(DataType dataType) {
		return set(TENSOR_DATA_TYPE, dataType);
	}

	default T setTensorDataType(String dataType) {
		return set(TENSOR_DATA_TYPE, ParamUtil.searchEnum(TENSOR_DATA_TYPE, dataType));
	}
}
