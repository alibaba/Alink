package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToKvParams;

/**
 * Transform data type from Json to Kv.
 */
@ParamSelectColumnSpec(name = "jsonCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("JSON转KV")
@NameEn("JSON To KV")
public class JsonToKvBatchOp extends BaseFormatTransBatchOp <JsonToKvBatchOp>
	implements JsonToKvParams <JsonToKvBatchOp> {

	private static final long serialVersionUID = 6230910672001928628L;

	public JsonToKvBatchOp() {
		this(new Params());
	}

	public JsonToKvBatchOp(Params params) {
		super(FormatType.JSON, FormatType.KV, params);
	}
}
