package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToKvParams;

/**
 * Transform data type from Triple to Kv.
 */
@NameCn("三元组转KV")
public class TripleToKvBatchOp extends TripleToAnyBatchOp <TripleToKvBatchOp>
	implements TripleToKvParams <TripleToKvBatchOp> {

	private static final long serialVersionUID = -8209013025023139624L;

	public TripleToKvBatchOp() {
		this(new Params());
	}

	public TripleToKvBatchOp(Params params) {
		super(FormatType.KV, params);
	}
}
