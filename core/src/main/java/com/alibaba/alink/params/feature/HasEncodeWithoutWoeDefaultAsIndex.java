package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasEncodeWithoutWoeDefaultAsIndex<T> extends WithParams <T> {

	@NameCn("编码方法")
	@DescCn("编码方法")
	ParamInfo <HasEncodeWithoutWoe.Encode> ENCODE = ParamInfoFactory
		.createParamInfo("encode", HasEncodeWithoutWoe.Encode.class)
		.setDescription("encode type: INDEX, VECTOR, ASSEMBLED_VECTOR.")
		.setHasDefaultValue(HasEncodeWithoutWoe.Encode.INDEX)
		.build();

	default HasEncodeWithoutWoe.Encode getEncode() {
		return get(ENCODE);
	}

	default T setEncode(HasEncodeWithoutWoe.Encode value) {
		return set(ENCODE, value);
	}

	default T setEncode(String value) {
		return set(ENCODE, ParamUtil.searchEnum(ENCODE, value));
	}
}
