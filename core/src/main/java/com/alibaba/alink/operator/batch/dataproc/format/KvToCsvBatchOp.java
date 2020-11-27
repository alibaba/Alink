package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToCsvParams;

/**
 * Transform data type from Kv to Csv.
 */
public class KvToCsvBatchOp extends BaseFormatTransBatchOp <KvToCsvBatchOp>
	implements KvToCsvParams <KvToCsvBatchOp> {

	private static final long serialVersionUID = -6846612393990957419L;

	public KvToCsvBatchOp() {
		this(new Params());
	}

	public KvToCsvBatchOp(Params params) {
		super(FormatType.KV, FormatType.CSV, params);
	}
}
