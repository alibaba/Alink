package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToJsonParams;

/**
 * Transform data type from Columns to Json.
 */
public class ColumnsToJsonStreamOp extends BaseFormatTransStreamOp <ColumnsToJsonStreamOp>
	implements ColumnsToJsonParams <ColumnsToJsonStreamOp> {

	private static final long serialVersionUID = 9043716900833099387L;

	public ColumnsToJsonStreamOp() {
		this(new Params());
	}

	public ColumnsToJsonStreamOp(Params params) {
		super(FormatType.COLUMNS, FormatType.JSON, params);
	}
}
