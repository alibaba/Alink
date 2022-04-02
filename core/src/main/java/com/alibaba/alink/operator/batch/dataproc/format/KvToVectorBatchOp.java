package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToVectorParams;

/**
 * Transform data type from Kv to Vector.
 */
@ParamSelectColumnSpec(name = "kvCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("KV转向量")
public class KvToVectorBatchOp extends BaseFormatTransBatchOp <KvToVectorBatchOp>
	implements KvToVectorParams <KvToVectorBatchOp> {

	private static final long serialVersionUID = -2154943001496309454L;

	public KvToVectorBatchOp() {
		this(new Params());
	}

	public KvToVectorBatchOp(Params params) {
		super(FormatType.KV, FormatType.VECTOR, params);
	}
}
