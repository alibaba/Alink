package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToCsvParams;

/**
 * Transform data type from Columns to Csv.
 */
public class ColumnsToCsvBatchOp extends BaseFormatTransBatchOp <ColumnsToCsvBatchOp>
	implements ColumnsToCsvParams <ColumnsToCsvBatchOp> {

	private static final long serialVersionUID = -3761261556546403806L;

	public ColumnsToCsvBatchOp() {
		this(new Params());
	}

	public ColumnsToCsvBatchOp(Params params) {
		super(FormatType.COLUMNS, FormatType.CSV, params);
	}
}
