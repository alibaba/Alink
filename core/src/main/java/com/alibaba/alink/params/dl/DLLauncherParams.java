package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.dl.DLEnvConfig.Version;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSchemaStr;

public interface DLLauncherParams<T> extends WithParams <T>,
	HasNumWorkersDefaultAsNull <T>, HasIntraOpParallelism <T>, HasNumPssDefaultAsNull <T>, HasUserFiles <T>,
	HasPythonEnv <T>, HasUserParams <T>, HasMainScriptFile <T>, HasOutputSchemaStr <T> {

	@NameCn("入口函数名称")
	@DescCn("入口函数名称")
	ParamInfo <String> ENTRY_FUNC = ParamInfoFactory
		.createParamInfo("entryFunc", String.class)
		.setDescription("entry func")
		.setRequired()
		.build();

	default String getEntryFunc() {
		return get(ENTRY_FUNC);
	}

	default T setEntryFunc(String value) {
		return set(ENTRY_FUNC, value);
	}

	@NameCn("环境版本号")
	@DescCn("环境版本号")
	ParamInfo <Version> ENV_VERSION = ParamInfoFactory
		.createParamInfo("envVersion", Version.class)
		.setDescription("environment version")
		.setOptional()
		.build();

	default Version getEnvVersion() {
		return get(ENV_VERSION);
	}

	default T setEnvVersion(Version value) {
		return set(ENV_VERSION, value);
	}
}
