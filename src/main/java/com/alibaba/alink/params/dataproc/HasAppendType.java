package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasAppendType<T> extends WithParams<T> {
	ParamInfo <String> APPEND_TYPE = ParamInfoFactory
		.createParamInfo("appendType", String.class)
		.setDescription("append type. DENSE or UNIQUE")
		.setHasDefaultValue(AppendIdBatchOp.AppendType.DENSE)
		.setAlias(new String[] {"AppendType"})
		.build();

	default String getAppendType() {
		return get(APPEND_TYPE);
	}

	default T setAppendType(String value) {
		return set(APPEND_TYPE, value);
	}
}
