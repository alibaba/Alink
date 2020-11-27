package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToTripleParams;

/**
 * Transform data type from Csv to Triple.
 */
public class CsvToTripleBatchOp extends AnyToTripleBatchOp <CsvToTripleBatchOp>
	implements CsvToTripleParams <CsvToTripleBatchOp> {

	private static final long serialVersionUID = -1764381249657181519L;

	public CsvToTripleBatchOp() {
		this(new Params());
	}

	public CsvToTripleBatchOp(Params params) {
		super(FormatType.CSV, params);
	}
}
