package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.LookupRedisMapper;
import com.alibaba.alink.params.dataproc.LookupRedisParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Lookup operation from redis.
 */
public class LookupRedis extends MapTransformer <LookupRedis>
	implements LookupRedisParams <LookupRedis> {

	public LookupRedis() {
		this(null);
	}

	public LookupRedis(Params params) {
		super(LookupRedisMapper::new, params);
	}

}