package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.utils.JsonPathMapper;
import com.alibaba.alink.params.dataproc.JsonValueParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Extract json value from json string.
 */
@NameCn("Json值抽取")
public class JsonValue extends MapTransformer <JsonValue>
	implements JsonValueParams <JsonValue> {

	public JsonValue() {
		this(null);
	}

	public JsonValue(Params params) {
		super(JsonPathMapper::new, params);
	}
}
