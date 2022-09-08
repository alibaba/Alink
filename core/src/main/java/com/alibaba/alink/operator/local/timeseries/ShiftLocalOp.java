package com.alibaba.alink.operator.local.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.ShiftMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.timeseries.ShiftParams;

@NameCn("Shift")
public class ShiftLocalOp extends MapLocalOp <ShiftLocalOp>
	implements ShiftParams <ShiftLocalOp> {

	public ShiftLocalOp() {
		this(null);
	}

	public ShiftLocalOp(Params params) {
		super(ShiftMapper::new, params);
	}

}
