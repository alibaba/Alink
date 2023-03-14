package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.SelectedColsWithFirstInputSpec;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.LookupRedisStringMapper;
import com.alibaba.alink.params.dataproc.LookupStringRedisParams;

/**
 * batch op for lookup String type key and value in redis.
 */
@SelectedColsWithFirstInputSpec
@NameCn("Redis 表查找String类型")
@NameEn("Lookup Redis Table For String")
public class LookupRedisStringBatchOp extends MapBatchOp <LookupRedisStringBatchOp>
	implements LookupStringRedisParams <LookupRedisStringBatchOp> {

	public LookupRedisStringBatchOp() {
		this(new Params());
	}

	public LookupRedisStringBatchOp(Params params) {
		super(LookupRedisStringMapper::new, params);
	}

}
