package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.AggLookupModelMapper;
import com.alibaba.alink.params.dataproc.AggLookupParams;

/**
 */
@NameCn("Agg表查找")
public class AggLookupBatchOp extends ModelMapBatchOp <AggLookupBatchOp>
	implements AggLookupParams <AggLookupBatchOp> {

	public AggLookupBatchOp() {
		this(null);
	}

	public AggLookupBatchOp(Params params) {
		super(AggLookupModelMapper::new, params);
	}
}
