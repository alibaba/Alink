package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.operator.common.timeseries.ExpandExtendedVarsMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.timeseries.ExpandExtendedVarsParams;

@Internal
public class ExpandExtendedVarsStreamOp extends MapStreamOp <ExpandExtendedVarsStreamOp>
	implements ExpandExtendedVarsParams <ExpandExtendedVarsStreamOp> {

	public ExpandExtendedVarsStreamOp() {
		this(new Params());
	}

	public ExpandExtendedVarsStreamOp(Params params) {
		super(ExpandExtendedVarsMapper::new, params);
	}
}
