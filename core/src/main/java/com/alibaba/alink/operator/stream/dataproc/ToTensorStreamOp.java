package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.ToTensorMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.ToTensorParams;

/**
 * stream op for transforming to tensor.
 */
public class ToTensorStreamOp extends MapStreamOp <ToTensorStreamOp>
	implements ToTensorParams <ToTensorStreamOp> {

	public ToTensorStreamOp() {
		this(new Params());
	}

	public ToTensorStreamOp(Params params) {
		super(ToTensorMapper::new, params);
	}

}
