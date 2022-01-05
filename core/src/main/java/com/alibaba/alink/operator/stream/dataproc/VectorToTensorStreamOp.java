package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.VectorToTensorMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.VectorToTensorParams;

/**
 * stream op for tensor to vector.
 */
@Deprecated
public class VectorToTensorStreamOp extends MapStreamOp <VectorToTensorStreamOp>
	implements VectorToTensorParams <VectorToTensorStreamOp> {

	public VectorToTensorStreamOp() {
		this(new Params());
	}

	public VectorToTensorStreamOp(Params params) {
		super(VectorToTensorMapper::new, params);
	}

}
