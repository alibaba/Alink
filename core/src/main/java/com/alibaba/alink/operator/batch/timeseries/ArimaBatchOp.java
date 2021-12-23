package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.timeseries.ArimaMapper;
import com.alibaba.alink.params.timeseries.ArimaParams;

public class ArimaBatchOp extends MapBatchOp <ArimaBatchOp>
	implements ArimaParams<ArimaBatchOp> {

	public ArimaBatchOp() {
		this(null);
	}

	public ArimaBatchOp(Params params) {
		super(ArimaMapper::new, params);
	}

}
