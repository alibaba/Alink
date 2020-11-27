package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToVectorParams;

/**
 * Transform data type from Csv to Vector.
 */
public class CsvToVectorBatchOp extends BaseFormatTransBatchOp <CsvToVectorBatchOp>
	implements CsvToVectorParams <CsvToVectorBatchOp> {

	private static final long serialVersionUID = 8595417565169304131L;

	public CsvToVectorBatchOp() {
		this(new Params());
	}

	public CsvToVectorBatchOp(Params params) {
		super(FormatType.CSV, FormatType.VECTOR, params);
	}
}
