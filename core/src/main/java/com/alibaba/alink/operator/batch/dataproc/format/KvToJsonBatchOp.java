package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToJsonParams;

/**
 * Transform data type from Kv to Json.
 */
public class KvToJsonBatchOp extends BaseFormatTransBatchOp <KvToJsonBatchOp>
	implements KvToJsonParams <KvToJsonBatchOp> {

	private static final long serialVersionUID = 5504835541940677768L;

	public KvToJsonBatchOp() {
		this(new Params());
	}

	public KvToJsonBatchOp(Params params) {
		super(FormatType.KV, FormatType.JSON, params);
	}
}
