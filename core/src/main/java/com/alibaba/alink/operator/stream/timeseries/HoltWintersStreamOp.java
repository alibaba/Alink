package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.timeseries.HoltWintersMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.timeseries.HoltWintersParams;

@NameCn("Holt-Winters")
@NameEn("Holt-Winters")
public class HoltWintersStreamOp extends MapStreamOp <HoltWintersStreamOp>
	implements HoltWintersParams <HoltWintersStreamOp> {

	private static final long serialVersionUID = -4244462494490385859L;

	public HoltWintersStreamOp() {
		this(null);
	}

	public HoltWintersStreamOp(Params params) {
		super(HoltWintersMapper::new, params);
	}
}
