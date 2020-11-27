package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToVectorParams;

/**
 * Transform data type from Columns to Vector.
 */
public class ColumnsToVectorBatchOp extends BaseFormatTransBatchOp <ColumnsToVectorBatchOp>
	implements ColumnsToVectorParams <ColumnsToVectorBatchOp> {

	private static final long serialVersionUID = -2294570294275668326L;

	public ColumnsToVectorBatchOp() {
		this(new Params());
	}

	public ColumnsToVectorBatchOp(Params params) {
		super(FormatType.COLUMNS, FormatType.VECTOR, params);
	}
}
