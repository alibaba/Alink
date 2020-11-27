package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToJsonParams;

/**
 * Transform data type from Columns to Json.
 */
public class ColumnsToJsonBatchOp extends BaseFormatTransBatchOp <ColumnsToJsonBatchOp>
	implements ColumnsToJsonParams <ColumnsToJsonBatchOp> {

	private static final long serialVersionUID = 6996718371147325226L;

	public ColumnsToJsonBatchOp() {
		this(new Params());
	}

	public ColumnsToJsonBatchOp(Params params) {
		super(FormatType.COLUMNS, FormatType.JSON, params);
	}
}
