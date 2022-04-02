package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.SelectedColsWithFirstInputSpec;
import com.alibaba.alink.operator.common.dataproc.LookupRedisMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.LookupRedisParams;

/**
 * stream op for lookup from redis.
 */
@SelectedColsWithFirstInputSpec
@NameCn("Redis表查找")
public class LookupRedisStreamOp extends MapStreamOp <LookupRedisStreamOp>
	implements LookupRedisParams <LookupRedisStreamOp> {

	public LookupRedisStreamOp() {
		this(new Params());
	}

	public LookupRedisStreamOp(Params params) {
		super(LookupRedisMapper::new, params);
	}

}
