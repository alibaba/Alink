package com.alibaba.alink.operator.local.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.HoltWintersMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.timeseries.HoltWintersParams;

@NameCn("HoltWinters")
public class HoltWintersLocalOp extends MapLocalOp <HoltWintersLocalOp>
	implements HoltWintersParams <HoltWintersLocalOp> {

	private static final long serialVersionUID = 3608585320017729444L;

	public HoltWintersLocalOp() {
		this(new Params());
	}

	public HoltWintersLocalOp(Params params) {
		super(HoltWintersMapper::new, params);
	}

}
