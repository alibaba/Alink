package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.utils.JsonPathMapper;
import com.alibaba.alink.operator.batch.utils.FlatMapBatchOp;
import com.alibaba.alink.params.dataproc.JsonValueParams;

/**
 * Extract json value from json string.
 */
public final class JsonValueBatchOp extends FlatMapBatchOp <JsonValueBatchOp>
	implements JsonValueParams <JsonValueBatchOp> {

	private static final long serialVersionUID = -8601726486010457142L;

	public JsonValueBatchOp() {
		this(null);
	}

	public JsonValueBatchOp(Params param) {
		super(JsonPathMapper::new, param);
	}
}
