package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToColumnsParams;

/**
 * Transform data type from Vector to Columns.
 */
public class VectorToColumnsBatchOp extends BaseFormatTransBatchOp <VectorToColumnsBatchOp>
	implements VectorToColumnsParams <VectorToColumnsBatchOp> {

	private static final long serialVersionUID = 156209692588746206L;

	public VectorToColumnsBatchOp() {
		this(new Params());
	}

	public VectorToColumnsBatchOp(Params params) {
		super(FormatType.VECTOR, FormatType.COLUMNS, params);
	}
}
