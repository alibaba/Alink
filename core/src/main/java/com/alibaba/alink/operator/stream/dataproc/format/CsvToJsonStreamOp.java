package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToJsonParams;

/**
 * Transform data type from Csv to Json.
 */
public class CsvToJsonStreamOp extends BaseFormatTransStreamOp <CsvToJsonStreamOp>
	implements CsvToJsonParams <CsvToJsonStreamOp> {

	private static final long serialVersionUID = 3903812781681430586L;

	public CsvToJsonStreamOp() {
		this(new Params());
	}

	public CsvToJsonStreamOp(Params params) {
		super(FormatType.CSV, FormatType.JSON, params);
	}
}
