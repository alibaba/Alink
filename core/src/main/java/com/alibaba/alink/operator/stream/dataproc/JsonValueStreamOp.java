package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.utils.JsonPathMapper;
import com.alibaba.alink.operator.stream.utils.FlatMapStreamOp;
import com.alibaba.alink.params.dataproc.JsonValueParams;

/**
 * Extract json value from json string.
 */
public class JsonValueStreamOp extends FlatMapStreamOp <JsonValueStreamOp>
	implements JsonValueParams <JsonValueStreamOp> {
	private static final long serialVersionUID = -4286462555983885273L;

	public JsonValueStreamOp() {this(null);}

	public JsonValueStreamOp(Params param) {
		super(JsonPathMapper::new, param);
	}

}
