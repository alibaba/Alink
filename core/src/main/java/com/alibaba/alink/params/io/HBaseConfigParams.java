package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.io.shared.HasPluginVersion;

public interface HBaseConfigParams<T> extends HasPluginVersion <T> {

	@NameCn("Zookeeper quorum")
	@DescCn("Zookeeper quorum 地址")
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

	@NameCn("HBase RPC 超时时间")
	@DescCn("HBase RPC 超时时间，单位毫秒")
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
