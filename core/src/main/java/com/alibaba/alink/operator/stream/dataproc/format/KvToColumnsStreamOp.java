package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToColumnsParams;

/**
 * Transform data type from Kv to Columns.
 */
public class KvToColumnsStreamOp extends BaseFormatTransStreamOp <KvToColumnsStreamOp>
	implements KvToColumnsParams <KvToColumnsStreamOp> {

	private static final long serialVersionUID = 141688056233402076L;

	public KvToColumnsStreamOp() {
		this(new Params());
	}

	public KvToColumnsStreamOp(Params params) {
		super(FormatType.KV, FormatType.COLUMNS, params);
	}
}
