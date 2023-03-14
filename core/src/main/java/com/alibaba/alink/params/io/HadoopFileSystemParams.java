package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.params.io.shared.HasFileSystemUri;
import com.alibaba.alink.params.io.shared.HasPluginVersion;

public interface HadoopFileSystemParams<T> extends HasFileSystemUri <T>, HasPluginVersion<T> {

	@NameCn("配置文件路径")
	@DescCn("配置文件路径")
	ParamInfo <String> CONFIGURATION_FILE_PATH = ParamInfoFactory
		.createParamInfo("configurationFilePath", String.class)
		.setDescription("Configure of the file system.")
		.setHasDefaultValue(null)
		.build();

	default FilePath getConfigurationFilePath() {
		return FilePath.deserialize(get(CONFIGURATION_FILE_PATH));
	}

	default T setConfigurationFilePath(FilePath value) {
		return set(CONFIGURATION_FILE_PATH, value.serialize());
	}

	@NameCn("配置")
	@DescCn("配置")
	ParamInfo <String> CONFIGURATION = ParamInfoFactory
		.createParamInfo("configure", String.class)
		.setDescription("Configure of the file system.")
		.setHasDefaultValue(null)
		.build();

	default String getConfiguration() {
		return get(CONFIGURATION);
	}

	default T setConfiguration(String value) {
		return set(CONFIGURATION, value);
	}
}
