package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.dl.HasIntraOpParallelism;
import com.alibaba.alink.params.dl.HasNumPssDefaultAsNull;
import com.alibaba.alink.params.dl.HasNumWorkersDefaultAsNull;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.dl.HasUserFiles;
import com.alibaba.alink.params.dl.HasUserParams;
import com.alibaba.alink.params.dl.HasTaskType;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

public interface EasyTransferConfigTrainParams<T> extends
	HasSelectedColsDefaultAsNull <T>, HasTaskType <T>,
	HasUserFiles<T>, HasUserParams<T>,
	HasPythonEnv <T>, HasIntraOpParallelism <T>,
	HasNumWorkersDefaultAsNull <T>, HasNumPssDefaultAsNull <T> {



	ParamInfo <String> CONFIG_JSON = ParamInfoFactory
		.createParamInfo("configJson", String.class)
		.setDescription("config in JSON format")
		.setRequired()
		.build();

	default String getConfigJson() {
		return get(CONFIG_JSON);
	}

	default T setConfigJson(String value) {
		return set(CONFIG_JSON, value);
	}
}
