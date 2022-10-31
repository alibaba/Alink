package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSparseFeatureDelimiterDefaultAsComma<T> extends WithParams <T> {
	@NameCn("稀疏特征分隔符")
	@DescCn("稀疏特征分隔符")
	ParamInfo <String> SPARSE_FEATURE_DELIMITER = ParamInfoFactory
		.createParamInfo("spareFeatureDelimiter", String.class)
		.setDescription("sparse feature delimiter")
		.setHasDefaultValue(",")
		.build();

	default String getSpareFeatureDelimiter() {
		return get(SPARSE_FEATURE_DELIMITER);
	}

	default T setSpareFeatureDelimiter(String value) {
		return set(SPARSE_FEATURE_DELIMITER, value);
	}
}
