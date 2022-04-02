package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.ShiftMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.timeseries.ShiftParams;

@NameCn("Shift")
public class ShiftStreamOp extends MapStreamOp <ShiftStreamOp>
	implements ShiftParams <ShiftStreamOp> {

	public ShiftStreamOp() {
		this(null);
	}

	public ShiftStreamOp(Params params) {
		super(ShiftMapper::new, params);
	}

}
