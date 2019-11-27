package com.alibaba.alink.operator.stream.dataproc;

import com.alibaba.alink.common.utils.JsonPathMapper;
import com.alibaba.alink.operator.stream.utils.FlatMapStreamOp;
import com.alibaba.alink.params.dataproc.JsonValueParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Extract json value from json string.
 */
public class JsonValueStreamOp extends FlatMapStreamOp<JsonValueStreamOp>
	implements JsonValueParams <JsonValueStreamOp> {
	public JsonValueStreamOp() {this(null);}

	public JsonValueStreamOp(Params param) {
		super(JsonPathMapper::new, param);
	}

}
