package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.FlatMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.FlattenMTableMapper;
import com.alibaba.alink.params.dataproc.FlattenMTableParams;

/**
 * Transform MTable format recommendation to table format.
 */
public class FlattenMTableBatchOp extends FlatMapBatchOp <FlattenMTableBatchOp>
	implements FlattenMTableParams <FlattenMTableBatchOp> {

	private static final long serialVersionUID = 790348573681664909L;

	public FlattenMTableBatchOp() {
		this(null);
	}

	public FlattenMTableBatchOp(Params params) {
		super(FlattenMTableMapper::new, params);
	}
}
