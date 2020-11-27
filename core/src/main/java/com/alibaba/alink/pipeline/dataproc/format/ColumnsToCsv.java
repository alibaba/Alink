package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToCsvParams;

/**
 * Transform data type from Columns to Csv.
 */
public class ColumnsToCsv extends BaseFormatTrans <ColumnsToCsv> implements ColumnsToCsvParams <ColumnsToCsv> {

	private static final long serialVersionUID = -1361356943130602122L;

	public ColumnsToCsv() {
		this(new Params());
	}

	public ColumnsToCsv(Params params) {
		super(FormatType.COLUMNS, FormatType.CSV, params);
	}
}

