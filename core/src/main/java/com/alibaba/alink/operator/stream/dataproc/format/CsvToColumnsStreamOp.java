package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToColumnsParams;

/**
 * Transform data type from Csv to Columns.
 */
public class CsvToColumnsStreamOp extends BaseFormatTransStreamOp <CsvToColumnsStreamOp>
	implements CsvToColumnsParams <CsvToColumnsStreamOp> {

	private static final long serialVersionUID = 8901824691602558430L;

	public CsvToColumnsStreamOp() {
		this(new Params());
	}

	public CsvToColumnsStreamOp(Params params) {
		super(FormatType.CSV, FormatType.COLUMNS, params);
	}
}
