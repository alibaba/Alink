package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.io.shared.HasPluginVersion;

public interface HBaseConfigParams<T> extends HasPluginVersion <T> {

	ParamInfo <String> ZOOKEEPER_QUORUM = ParamInfoFactory.createParamInfo("zookeeperQuorum", String.class)
		.setDescription("zookeeper quorum")
		.setRequired()
		.build();

	default String getZookeeperQuorum() {
		return get(ZOOKEEPER_QUORUM);
	}

	default T setZookeeperQuorum(String value) {
		return set(ZOOKEEPER_QUORUM, value);
	}

	ParamInfo <Integer> TIMEOUT = ParamInfoFactory.createParamInfo("timeout", Integer.class)
		.setDescription(
			"hbase rpc timeout, millisecond")
		.setHasDefaultValue(1000)
		.build();

	default Integer getTimeout() {
		return get(TIMEOUT);
	}

	default T setTimeout(int value) {
		return set(TIMEOUT, value);
	}
}
