package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.pipeline.dataproc.IndexToString;

/**
 * Parameters for {@link IndexToString}.
 */
public interface IndexToStringPredictParams<T> extends
	ModelMapperParams <T>,
	HasSelectedCol <T>,
	HasReservedColsDefaultAsNull <T>,
	HasOutputColDefaultAsNull <T> {

	/**
	 * @cn-name 模型名字
	 * @cn 模型名字
	 */
	ParamInfo <String> MODEL_NAME = ParamInfoFactory
		.createParamInfo("modelName", String.class)
		.setDescription("Name of the model")
		.setRequired()
		.build();

	default String getModelName() {
		return get(MODEL_NAME);
	}

	default T setModelName(String colName) {
		return set(MODEL_NAME, colName);
	}
}