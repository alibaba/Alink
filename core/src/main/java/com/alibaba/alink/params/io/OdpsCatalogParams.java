package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.io.shared.HasAccessId;
import com.alibaba.alink.params.io.shared.HasAccessKey;
import com.alibaba.alink.params.io.shared.HasCatalogName;
import com.alibaba.alink.params.io.shared.HasDefaultDatabase;
import com.alibaba.alink.params.io.shared.HasEndPoint;
import com.alibaba.alink.params.io.shared.HasPluginVersion;
import com.alibaba.alink.params.io.shared.HasProject;

public interface OdpsCatalogParams<T> extends WithParams <T>,
	HasAccessId <T>, HasAccessKey <T>, HasProject <T>,
	HasEndPoint <T>, HasCatalogName <T>, HasDefaultDatabase <T>,
	HasPluginVersion<T> {

	ParamInfo <String> RUNNING_PROJECT = ParamInfoFactory
		.createParamInfo("runningProject", String.class)
		.setDescription("the project name for executing sql command")
		.setOptional()
		.build();

	default String getRunningProject() {
		return get(RUNNING_PROJECT);
	}

	default T setRunningProject(String value) {
		return set(RUNNING_PROJECT, value);
	}
}
