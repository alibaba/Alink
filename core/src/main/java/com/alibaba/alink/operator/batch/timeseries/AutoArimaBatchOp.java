package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.timeseries.AutoArimaMapper;
import com.alibaba.alink.params.timeseries.AutoArimaParams;

public final class AutoArimaBatchOp extends MapBatchOp <AutoArimaBatchOp>
	implements AutoArimaParams <AutoArimaBatchOp> {

	private static final long serialVersionUID = 7262689742394398554L;

	public AutoArimaBatchOp() {
		this(null);
	}

	public AutoArimaBatchOp(Params params) {
		super(AutoArimaMapper::new, params);
	}
}
