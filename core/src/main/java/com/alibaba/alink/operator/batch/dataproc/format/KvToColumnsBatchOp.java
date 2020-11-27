package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToColumnsParams;

/**
 * Transform data type from Kv to Columns.
 */
public class KvToColumnsBatchOp extends BaseFormatTransBatchOp <KvToColumnsBatchOp>
	implements KvToColumnsParams <KvToColumnsBatchOp> {

	private static final long serialVersionUID = 903562921542569706L;

	public KvToColumnsBatchOp() {
		this(new Params());
	}

	public KvToColumnsBatchOp(Params params) {
		super(FormatType.KV, FormatType.COLUMNS, params);
	}
}
