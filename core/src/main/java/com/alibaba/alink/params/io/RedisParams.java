package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.io.shared.HasPluginVersion;

public interface RedisParams<T> extends HasPluginVersion <T> {

	ParamInfo <String> REDIS_IP = ParamInfoFactory.createParamInfo("redisIP", String.class)
		.setDescription("the IP of the Redis server.")
		.setRequired()
		.build();

	default String getRedisIP() {
		return get(REDIS_IP);
	}

	default T setRedisIP(String value) {
		return set(REDIS_IP, value);
	}

	ParamInfo <Integer> REDIS_PORT = ParamInfoFactory.createParamInfo("redisPort", Integer.class)
		.setDescription("the port of the Redis server.")
		.setHasDefaultValue(6379)
		.build();

	default Integer getRedisPort() {
		return get(REDIS_PORT);
	}

	default T setRedisPort(Integer value) {
		return set(REDIS_PORT, value);
	}

	ParamInfo <String> REDIS_PASSWORD = ParamInfoFactory.createParamInfo("redisPassword", String.class)
		.setDescription("the password of the Redis server.")
		.build();

	default String getRedisPassword() {
		return get(REDIS_PASSWORD);
	}

	default T setRedisPassword(String value) {
		return set(REDIS_PASSWORD, value);
	}
}
