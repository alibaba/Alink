package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.LookupRedisMapper;
import com.alibaba.alink.params.dataproc.LookupRedisParams;

/**
 * stream op for lookup from redis.
 */
public class LookupRedisBatchOp extends MapBatchOp <LookupRedisBatchOp>
	implements LookupRedisParams <LookupRedisBatchOp> {

	public LookupRedisBatchOp() {
		this(new Params());
	}

	public LookupRedisBatchOp(Params params) {
		super(LookupRedisMapper::new, params);
	}

}
