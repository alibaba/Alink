package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.AggLookupModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.AggLookupParams;

/**
 */
@NameCn("Agg表查找")
@NameEn("Agg Lookup")
public class AggLookupStreamOp extends ModelMapStreamOp <AggLookupStreamOp>
	implements AggLookupParams <AggLookupStreamOp> {

	public AggLookupStreamOp() {
		super(AggLookupModelMapper::new, new Params());
	}

	public AggLookupStreamOp(Params params) {
		super(AggLookupModelMapper::new, params);
	}

	public AggLookupStreamOp(BatchOperator model) {
		this(model, null);
	}

	public AggLookupStreamOp(BatchOperator model, Params params) {
		super(model, AggLookupModelMapper::new, params);
	}
}
