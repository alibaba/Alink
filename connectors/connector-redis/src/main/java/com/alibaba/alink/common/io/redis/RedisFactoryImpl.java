package com.alibaba.alink.common.io.redis;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.io.RedisParams;
import redis.clients.jedis.Jedis;

public class RedisFactoryImpl implements RedisFactory {

	@Override
	public Redis create(Params params) {

		final Jedis redis = new Jedis(params.get(RedisParams.REDIS_IP), params.get(RedisParams.REDIS_PORT));

		if (params.contains(RedisParams.REDIS_PASSWORD)) {
			redis.auth(params.get(RedisParams.REDIS_PASSWORD));
		}

		return new Redis() {
			@Override
			public void close() {
				redis.close();
			}

			@Override
			public String ping() {
				return redis.ping();
			}

			@Override
			public String set(byte[] key, byte[] value) {
				return redis.set(key, value);
			}

			@Override
			public byte[] get(byte[] key) {
				return redis.get(key);
			}
		};
	}
}
