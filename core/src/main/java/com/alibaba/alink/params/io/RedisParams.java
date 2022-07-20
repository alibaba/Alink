package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.io.shared.HasPluginVersion;

public interface RedisParams<T> extends HasPluginVersion <T> {
	/**
	 * REDIS_IP, REDIS_PORT, DATABASE_INDEX and TIMEOUT only works in cluster mode REDIS_IPS only works in cluster mode,
	 * the format is "ip:port,ip:port,..."
	 */
	@NameCn("Redis 密码")
	@DescCn("Redis 服务器密码")
	ParamInfo <String> REDIS_PASSWORD = ParamInfoFactory.createParamInfo("redisPassword", String.class)
		.setDescription("the password of the Redis server.")
		.build();

	@NameCn("数据库索引号")
	@DescCn("数据库索引号")
	ParamInfo <Long> DATABASE_INDEX = ParamInfoFactory.createParamInfo("databaseIndex", Long.class)
		.setDescription("the index of the Redis db.")
		.build();

	@NameCn("超时")
	@DescCn("关闭连接的超时时间")
	ParamInfo <Integer> TIMEOUT = ParamInfoFactory.createParamInfo("timeout", Integer.class)
		.setDescription(
			"close the connection if the client is idle for timeout seconds.")
		.build();

	@NameCn("Redis IP")
	@DescCn("Redis 集群的 IP/端口")
	ParamInfo <String[]> REDIS_IPS = ParamInfoFactory.createParamInfo("redisIPs", String[].class)
		.setDescription("IPs and ports of the Redis cluster.")
		.build();

	@NameCn("集群模式")
	@DescCn("是集群模式还是单机模式")
	ParamInfo <Boolean> CLUSTER_MODE = ParamInfoFactory.createParamInfo("clusterMode", Boolean.class)
		.setDescription("Redis running in cluster or standalone mode.")
		.setHasDefaultValue(false)
		.build();

	@NameCn("流水线大小")
	@DescCn("Redis 发送命令流水线的大小")
	ParamInfo <Integer> PIPELINE_SIZE = ParamInfoFactory.createParamInfo("pipelineSize", Integer.class)
		.setDescription("Redis sends commands using pipelining.")
		.setHasDefaultValue(1)
		.build();

	default Boolean getClusterMode() {
		return get(CLUSTER_MODE);
	}

	default T setClusterMode(Boolean value) {
		return set(CLUSTER_MODE, value);
	}

	default String[] getRedisIPs() {
		return get(REDIS_IPS);
	}

	default T setRedisIPs(String... value) {
		return set(REDIS_IPS, value);
	}

	default String getRedisPassword() {
		return get(REDIS_PASSWORD);
	}

	default T setRedisPassword(String value) {
		return set(REDIS_PASSWORD, value);
	}

	default Long getDatabaseIndex() {
		return get(DATABASE_INDEX);
	}

	default T setDatabaseIndex(long value) {
		return set(DATABASE_INDEX, value);
	}
	default Integer getTimeout() {
		return get(TIMEOUT);
	}

	default T setTimeout(int value) {
		return set(TIMEOUT, value);
	}

	default T setPipelineSize(int value) {return set(PIPELINE_SIZE, value);}
	default int getPipelineSize() {return get(PIPELINE_SIZE);}
}
