package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.onlinelearning.ModelFilterParams;

/**
 */
@NameCn("Ftrl模型过滤")
@NameEn("Follow the regularized leader model filter")
public final class FtrlModelFilterStreamOp extends BinaryClassModelFilterStreamOp<FtrlModelFilterStreamOp>
	implements ModelFilterParams<FtrlModelFilterStreamOp> {

	public FtrlModelFilterStreamOp() {
		this(new Params());
	}

	public FtrlModelFilterStreamOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
