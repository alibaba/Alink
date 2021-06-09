package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.onlinelearning.ModelFilterParams;

/**
 */
public final class FtrlModelFilterStreamOp extends BinaryClassModelFilterStreamOp<FtrlModelFilterStreamOp>
	implements ModelFilterParams<FtrlModelFilterStreamOp> {

	public FtrlModelFilterStreamOp() {
		this(new Params());
	}

	public FtrlModelFilterStreamOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
