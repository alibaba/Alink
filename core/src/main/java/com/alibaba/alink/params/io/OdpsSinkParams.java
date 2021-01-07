package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.io.shared.HasLifeCycleDefaultAsNeg1;
import com.alibaba.alink.params.io.shared.HasOutputTableName;

public interface OdpsSinkParams<T> extends
	OdpsCatalogParams <T>,
	HasOutputTableName <T>,
	HasLifeCycleDefaultAsNeg1 <T> {

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

	ParamInfo <Boolean> OVERWRITE_SINK = ParamInfoFactory
		.createParamInfo("overwriteSink", Boolean.class)
		.setDescription("Whether to overwrite existing data.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getOverwriteSink() {
		return get(OVERWRITE_SINK);
	}

	default T setOverwriteSink(Boolean value) {
		return set(OVERWRITE_SINK, value);
	}
}


