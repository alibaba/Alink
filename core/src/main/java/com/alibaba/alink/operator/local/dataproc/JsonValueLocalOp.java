package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.JsonPathMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.JsonValueParams;

/**
 * Extract json value from json string.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("JSON值抽取")
@NameEn("Json Value Extraction")
public final class JsonValueLocalOp extends MapLocalOp <JsonValueLocalOp>
	implements JsonValueParams <JsonValueLocalOp> {

	public JsonValueLocalOp() {
		this(null);
	}

	public JsonValueLocalOp(Params param) {
		super(JsonPathMapper::new, param);
	}
}
