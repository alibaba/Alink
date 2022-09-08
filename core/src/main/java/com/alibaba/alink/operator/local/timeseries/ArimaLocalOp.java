package com.alibaba.alink.operator.local.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.ArimaMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.timeseries.ArimaParams;

@NameCn("Arima")
public class ArimaLocalOp extends MapLocalOp <ArimaLocalOp>
	implements ArimaParams <ArimaLocalOp> {

	public ArimaLocalOp() {
		this(null);
	}

	public ArimaLocalOp(Params params) {
		super(ArimaMapper::new, params);
	}

}
