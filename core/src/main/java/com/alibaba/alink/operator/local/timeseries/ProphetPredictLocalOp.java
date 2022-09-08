package com.alibaba.alink.operator.local.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.ProphetModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.timeseries.ProphetPredictParams;

@NameCn("Prophet预测")
public class ProphetPredictLocalOp extends ModelMapLocalOp <ProphetPredictLocalOp>
	implements ProphetPredictParams <ProphetPredictLocalOp> {

	public ProphetPredictLocalOp() {
		this(null);
	}

	public ProphetPredictLocalOp(Params params) {
		super(ProphetModelMapper::new, params);
	}

}
