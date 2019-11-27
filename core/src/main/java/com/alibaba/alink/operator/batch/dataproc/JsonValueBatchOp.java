package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.common.utils.JsonPathMapper;
import com.alibaba.alink.operator.batch.utils.FlatMapBatchOp;
import com.alibaba.alink.params.dataproc.JsonValueParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Extract json value from json string.
 */
public final class JsonValueBatchOp extends FlatMapBatchOp<JsonValueBatchOp>
	implements JsonValueParams <JsonValueBatchOp> {

	public JsonValueBatchOp() {
		this(null);
	}

	public JsonValueBatchOp(Params param) {
		super(JsonPathMapper::new, param);
	}
}
