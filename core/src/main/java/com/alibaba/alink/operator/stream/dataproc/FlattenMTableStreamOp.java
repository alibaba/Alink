package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.FlatMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.FlattenMTableMapper;
import com.alibaba.alink.operator.stream.utils.FlatMapStreamOp;
import com.alibaba.alink.params.dataproc.FlattenMTableParams;

/**
 * Transform MTable format recommendation to table format.
 */
public class FlattenMTableStreamOp extends FlatMapStreamOp <FlattenMTableStreamOp>
	implements FlattenMTableParams <FlattenMTableStreamOp> {

	private static final long serialVersionUID = 790348573681664909L;

	public FlattenMTableStreamOp() {
		this(null);
	}

	public FlattenMTableStreamOp(Params params) {
		super(FlattenMTableMapper::new, params);
	}
}
