package com.alibaba.alink.params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface PipelineModelParams<T> extends
	ModelStreamScanParams <T> {

	@NameCn("trainingDataSchema")
	@DescCn("Input training data schema of the pipeline model.")
	ParamInfo <String> TRAINING_DATA_SCHEMA = ParamInfoFactory
		.createParamInfo("trainingDataSchema", String.class)
		.setDescription("Input training data schema of the pipeline model.")
		.setHasDefaultValue(null)
		.build();

	default T setTrainDataSchema(String schemaStr) {
		return set(TRAINING_DATA_SCHEMA, schemaStr);
	}

	default String getTrainDataSchema() {
		return get(TRAINING_DATA_SCHEMA);
	}

	@NameCn("timestamp")
	@DescCn("Timestamp of the pipeline model.")
	ParamInfo <String> TIMESTAMP = ParamInfoFactory
		.createParamInfo("timestamp", String.class)
		.setDescription("Timestamp of the pipeline model.")
		.setHasDefaultValue(null)
		.build();
}