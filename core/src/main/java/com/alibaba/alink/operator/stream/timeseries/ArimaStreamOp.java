package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.ArimaMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.timeseries.ArimaParams;

@NameCn("Arima")
public class ArimaStreamOp extends MapStreamOp <ArimaStreamOp>
	implements ArimaParams<ArimaStreamOp> {

	public ArimaStreamOp() {
		this(null);
	}

	public ArimaStreamOp(Params params) {
		super(ArimaMapper::new, params);
	}

}
