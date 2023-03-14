package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToTripleParams;

/**
 * Transform data type from Kv to Triple.
 */
@ParamSelectColumnSpec(name = "kvCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("KV转三元组")
@NameEn("KV To Triple")
public class KvToTripleBatchOp extends AnyToTripleBatchOp <KvToTripleBatchOp>
	implements KvToTripleParams <KvToTripleBatchOp> {

	private static final long serialVersionUID = -9207851767735374588L;

	public KvToTripleBatchOp() {
		this(new Params());
	}

	public KvToTripleBatchOp(Params params) {
		super(FormatType.KV, params);
	}
}
