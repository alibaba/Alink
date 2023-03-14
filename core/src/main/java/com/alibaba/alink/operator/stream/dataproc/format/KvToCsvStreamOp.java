package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToCsvParams;

/**
 * Transform data type from Kv to Csv.
 */
@ParamSelectColumnSpec(name = "kvCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("KV转CSV")
@NameEn("Key-value pairs to csv")
public class KvToCsvStreamOp extends BaseFormatTransStreamOp <KvToCsvStreamOp>
	implements KvToCsvParams <KvToCsvStreamOp> {

	private static final long serialVersionUID = 1364131618084402185L;

	public KvToCsvStreamOp() {
		this(new Params());
	}

	public KvToCsvStreamOp(Params params) {
		super(FormatType.KV, FormatType.CSV, params);
	}
}
