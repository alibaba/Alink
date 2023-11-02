package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.LookupRedisRowMapper;
import com.alibaba.alink.params.dataproc.LookupRedisRowParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Lookup operation from redis.
 */
@NameCn("Redis表查找")
public class LookupRedisRow extends MapTransformer <LookupRedisRow>
	implements LookupRedisRowParams<LookupRedisRow> {

	public LookupRedisRow() {
		this(null);
	}

	public LookupRedisRow(Params params) {
		super(LookupRedisRowMapper::new, params);
	}

}
