package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToTripleParams;

/**
 * Transform data type from Csv to Triple.
 */
public class CsvToTripleStreamOp extends AnyToTripleStreamOp <CsvToTripleStreamOp>
	implements CsvToTripleParams <CsvToTripleStreamOp> {

	private static final long serialVersionUID = 4904881842860650157L;

	public CsvToTripleStreamOp() {
		this(new Params());
	}

	public CsvToTripleStreamOp(Params params) {
		super(FormatType.CSV, params);
	}
}
