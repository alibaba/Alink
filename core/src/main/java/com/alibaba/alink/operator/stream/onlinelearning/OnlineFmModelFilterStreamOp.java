package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.fm.FmModelMapper;
import com.alibaba.alink.params.onlinelearning.ModelFilterParams;

/**
 * Online Fm model filter.
 */

@Internal
@NameCn("在线FM过滤")
public final class OnlineFmModelFilterStreamOp extends BinaryClassModelFilterStreamOp<OnlineFmModelFilterStreamOp>
	implements ModelFilterParams<OnlineFmModelFilterStreamOp> {

	public OnlineFmModelFilterStreamOp() {
		this(new Params());
	}

	public OnlineFmModelFilterStreamOp(Params params) {
		super(FmModelMapper::new, params);
	}
}
