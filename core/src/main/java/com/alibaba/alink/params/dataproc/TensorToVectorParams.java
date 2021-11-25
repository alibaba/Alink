package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * parameters of tensor to vector.
 */
public interface TensorToVectorParams<T> extends SISOMapperParams<T>{
	/**
	 * @cn-name 转换方法
	 * @cn 张量转换为向量的方法，可取 flatten, sum, mean, max, min.
	 */
	ParamInfo<ConvertMethod> CONVERT_METHOD = ParamInfoFactory
		.createParamInfo("convertMethod", ConvertMethod.class)
		.setDescription("the convert method of tensor to vector. include： flatten, sum, mean, max, min.")
		.setHasDefaultValue(ConvertMethod.FLATTEN)
		.build();

	default ConvertMethod getConvertMethod() {
		return get(CONVERT_METHOD);
	}

	default T setConvertMethod(ConvertMethod value) {
		return set(CONVERT_METHOD, value);
	}

	default T setConvertMethod(String value) {
		return set(CONVERT_METHOD, ParamUtil.searchEnum(CONVERT_METHOD, value));
	}

	enum ConvertMethod {
		FLATTEN,
		SUM,
		MEAN,
		MAX,
		MIN,
	}
}