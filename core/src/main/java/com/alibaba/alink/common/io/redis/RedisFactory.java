package com.alibaba.alink.common.io.redis;

import org.apache.flink.ml.api.misc.param.Params;

public interface RedisFactory {
	Redis create(Params params);
}
