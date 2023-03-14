package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.timeseries.AutoArimaMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.timeseries.AutoArimaParams;

@NameCn("Auto ARIMA")
@NameEn("Auto ARIMA")
public final class AutoArimaStreamOp extends MapStreamOp <AutoArimaStreamOp>
	implements AutoArimaParams<AutoArimaStreamOp> {

	private static final long serialVersionUID = -1748693000523419794L;

	public AutoArimaStreamOp() {
		this(null);
	}

	public AutoArimaStreamOp(Params params) {
		super(AutoArimaMapper::new, params);
	}
}
