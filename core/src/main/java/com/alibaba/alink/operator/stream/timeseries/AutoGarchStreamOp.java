package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.timeseries.AutoGarchMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.timeseries.AutoGarchParams;

public class AutoGarchStreamOp extends MapStreamOp <AutoGarchStreamOp>
	implements AutoGarchParams <AutoGarchStreamOp> {

	public AutoGarchStreamOp() {
		this(null);
	}

	public AutoGarchStreamOp(Params params) {
		super(AutoGarchMapper::new, params);
	}
}
