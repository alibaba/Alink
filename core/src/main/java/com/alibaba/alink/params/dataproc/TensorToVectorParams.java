package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * parameters of tensor to vector.
 */
public interface TensorToVectorParams<T> extends SISOMapperParams <T> {
	@NameCn("转换方法")
	@DescCn("张量转换为向量的方法，可取 flatten, sum, mean, max, min.")
	ParamInfo <ConvertMethod> CONVERT_METHOD = ParamInfoFactory
		.createParamInfo("convertMethod", ConvertMethod.class)
		.setDescription("the convert method of tensor to vector. include： flatten, sum, mean, max, min.")
		.setHasDefaultValue(ConvertMethod.FLATTEN)
		.build();

	default ConvertMethod getConvertMethod() {
		return get(CONVERT_METHOD);
	}

	default T setConvertMethod(ConvertMethod method) {
		return set(CONVERT_METHOD, method);
	}

	default T setConvertMethod(String method) {
		return set(CONVERT_METHOD, ParamUtil.searchEnum(CONVERT_METHOD, method));
	}

	enum ConvertMethod {
		FLATTEN,
		SUM,
		MEAN,
		MAX,
		MIN,
	}
}