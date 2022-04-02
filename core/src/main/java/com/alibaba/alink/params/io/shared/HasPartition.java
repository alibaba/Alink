package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasPartition<T> extends WithParams <T> {
	@NameCn("分区名")
	@DescCn("例如：ds=20190729/dt=12")
	ParamInfo <String> PARTITION = ParamInfoFactory
		.createParamInfo("partition", String.class)
		.setDescription("partition")
		.setHasDefaultValue(null)
		.build();

	default String getPartition() {
		return get(PARTITION);
	}

	default T setPartition(String value) {
		return set(PARTITION, value);
	}
}
