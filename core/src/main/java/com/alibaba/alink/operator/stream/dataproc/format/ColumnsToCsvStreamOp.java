package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToCsvParams;

/**
 * Transform data type from Columns to Csv.
 */
public class ColumnsToCsvStreamOp extends BaseFormatTransStreamOp <ColumnsToCsvStreamOp>
	implements ColumnsToCsvParams <ColumnsToCsvStreamOp> {

	private static final long serialVersionUID = 4938768662740660939L;

	public ColumnsToCsvStreamOp() {
		this(new Params());
	}

	public ColumnsToCsvStreamOp(Params params) {
		super(FormatType.COLUMNS, FormatType.CSV, params);
	}
}
