package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.SelectedColsWithFirstInputSpec;
import com.alibaba.alink.operator.common.dataproc.LookupRedisStringMapper;
import com.alibaba.alink.params.dataproc.LookupStringRedisParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * lookup String type key and value in redis.
 */
@SelectedColsWithFirstInputSpec
@NameCn("Redis 表查找String类型")
public class LookupStringRedis extends MapTransformer <LookupStringRedis>
	implements LookupStringRedisParams <LookupStringRedis> {

	public LookupStringRedis() {
		this(new Params());
	}

	public LookupStringRedis(Params params) {
		super(LookupRedisStringMapper::new, params);
	}

}
