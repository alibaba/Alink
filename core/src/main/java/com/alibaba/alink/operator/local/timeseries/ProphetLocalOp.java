package com.alibaba.alink.operator.local.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.ProphetMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.timeseries.ProphetParams;

@NameCn("Prophet")
public class ProphetLocalOp extends MapLocalOp <ProphetLocalOp>
	implements ProphetParams <ProphetLocalOp> {

	public ProphetLocalOp() {
		this(null);
	}

	public ProphetLocalOp(Params params) {
		super(ProphetMapper::new, params);
	}

}
