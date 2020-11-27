package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToCsvParams;

/**
 * Transform data type from Triple to Csv.
 */
public class TripleToCsvBatchOp extends TripleToAnyBatchOp <TripleToCsvBatchOp>
	implements TripleToCsvParams <TripleToCsvBatchOp> {

	private static final long serialVersionUID = 7557633497922961728L;

	public TripleToCsvBatchOp() {
		this(new Params());
	}

	public TripleToCsvBatchOp(Params params) {
		super(FormatType.CSV, params);
	}
}
