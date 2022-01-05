package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.ToMTableMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.ToMTableParams;

/**
 * stream op for transforming to MTable.
 */
public class ToMTableStreamOp extends MapStreamOp <ToMTableStreamOp>
	implements ToMTableParams <ToMTableStreamOp> {

	public ToMTableStreamOp() {
		this(new Params());
	}

	public ToMTableStreamOp(Params params) {
		super(ToMTableMapper::new, params);
	}

}
