package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToVectorParams;

/**
 * Transform data type from Csv to Vector.
 */
public class CsvToVectorStreamOp extends BaseFormatTransStreamOp <CsvToVectorStreamOp>
	implements CsvToVectorParams <CsvToVectorStreamOp> {

	private static final long serialVersionUID = 5496237228934299784L;

	public CsvToVectorStreamOp() {
		this(new Params());
	}

	public CsvToVectorStreamOp(Params params) {
		super(FormatType.CSV, FormatType.VECTOR, params);
	}
}
