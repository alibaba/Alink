package com.alibaba.alink.operator.local.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.AutoArimaMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.timeseries.AutoArimaParams;

@NameCn("AutoArima")
public final class AutoArimaLocalOp extends MapLocalOp <AutoArimaLocalOp>
	implements AutoArimaParams <AutoArimaLocalOp> {

	private static final long serialVersionUID = 7262689742394398554L;

	public AutoArimaLocalOp() {
		this(null);
	}

	public AutoArimaLocalOp(Params params) {
		super(AutoArimaMapper::new, params);
	}
}
