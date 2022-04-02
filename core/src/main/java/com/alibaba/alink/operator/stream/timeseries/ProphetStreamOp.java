package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.ProphetMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.timeseries.ProphetParams;

@NameCn("Prophet")
public class ProphetStreamOp extends MapStreamOp <ProphetStreamOp>
	implements ProphetParams <ProphetStreamOp> {

	public ProphetStreamOp() {
		this(null);
	}

	public ProphetStreamOp(Params params) {
		super(ProphetMapper::new, params);
	}

}
