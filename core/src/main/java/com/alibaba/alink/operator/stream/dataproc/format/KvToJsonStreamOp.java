package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToJsonParams;

/**
 * Transform data type from Kv to Json.
 */
@ParamSelectColumnSpec(name = "kvCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("KV转JSON")
public class KvToJsonStreamOp extends BaseFormatTransStreamOp <KvToJsonStreamOp>
	implements KvToJsonParams <KvToJsonStreamOp> {

	private static final long serialVersionUID = 5167041026296967441L;

	public KvToJsonStreamOp() {
		this(new Params());
	}

	public KvToJsonStreamOp(Params params) {
		super(FormatType.KV, FormatType.JSON, params);
	}
}
