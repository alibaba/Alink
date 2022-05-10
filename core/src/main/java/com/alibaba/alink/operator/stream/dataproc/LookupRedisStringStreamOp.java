package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.SelectedColsWithFirstInputSpec;
import com.alibaba.alink.operator.common.dataproc.LookupRedisStringMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.LookupStringRedisParams;

/**
 * stream op for lookup String type key and value in redis.
 */
@SelectedColsWithFirstInputSpec
@NameCn("Redis 表查找String类型")
public class LookupRedisStringStreamOp extends MapStreamOp <LookupRedisStringStreamOp>
	implements LookupStringRedisParams <LookupRedisStringStreamOp> {

	public LookupRedisStringStreamOp() {
		this(new Params());
	}

	public LookupRedisStringStreamOp(Params params) {
		super(LookupRedisStringMapper::new, params);
	}

}
