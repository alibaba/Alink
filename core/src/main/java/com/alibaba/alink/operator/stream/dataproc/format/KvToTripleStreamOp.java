package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToTripleParams;

/**
 * Transform data type from Kv to Triple.
 */
public class KvToTripleStreamOp extends AnyToTripleStreamOp <KvToTripleStreamOp>
	implements KvToTripleParams <KvToTripleStreamOp> {

	private static final long serialVersionUID = 6753289776004838551L;

	public KvToTripleStreamOp() {
		this(new Params());
	}

	public KvToTripleStreamOp(Params params) {
		super(FormatType.KV, params);
	}
}
