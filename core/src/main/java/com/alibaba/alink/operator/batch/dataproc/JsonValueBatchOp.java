package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.JsonPathMapper;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.params.dataproc.JsonValueParams;

/**
 * Extract json value from json string.
 */
@ParamSelectColumnSpec(name="selectedCol",allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("JSON值抽取")
@NameEn("Json Value Extraction")
public final class JsonValueBatchOp extends MapBatchOp <JsonValueBatchOp>
	implements JsonValueParams <JsonValueBatchOp> {

	private static final long serialVersionUID = -8601726486010457142L;

	public JsonValueBatchOp() {
		this(null);
	}

	public JsonValueBatchOp(Params param) {
		super(JsonPathMapper::new, param);
	}
}
