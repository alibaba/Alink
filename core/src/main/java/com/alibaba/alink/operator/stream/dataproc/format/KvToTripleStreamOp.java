package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToTripleParams;

/**
 * Transform data type from Kv to Triple.
 */
@ParamSelectColumnSpec(name = "kvCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("KV转三元组")
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
