package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasIndices<T> extends WithParams <T> {

	@NameCn("需要被提取的索引数组")
	@DescCn("需要被提取的索引数组")
	ParamInfo <int[]> INDICES = ParamInfoFactory
		.createParamInfo("indices", int[].class)
		.setDescription("indices of a vector to be sliced")
		.setHasDefaultValue(null)
		.build();

	default int[] getIndices() {
		return get(INDICES);
	}

	default T setIndices(int[] value) {
		return set(INDICES, value);
	}
}
