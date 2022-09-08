package com.alibaba.alink.operator.local.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.LSTNetModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.timeseries.LSTNetPredictParams;

@NameCn("LSTNet预测")
public class LSTNetPredictLocalOp extends ModelMapLocalOp <LSTNetPredictLocalOp>
	implements LSTNetPredictParams <LSTNetPredictLocalOp> {

	public LSTNetPredictLocalOp() {
		this(new Params());
	}

	public LSTNetPredictLocalOp(Params params) {
		super(LSTNetModelMapper::new, params);
	}
}
