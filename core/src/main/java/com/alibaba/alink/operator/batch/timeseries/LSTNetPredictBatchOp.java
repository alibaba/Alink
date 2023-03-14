package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.timeseries.LSTNetModelMapper;
import com.alibaba.alink.params.timeseries.LSTNetPredictParams;

@NameCn("LSTNet预测")
@NameEn("LSTNet Prediction")
public class LSTNetPredictBatchOp extends ModelMapBatchOp <LSTNetPredictBatchOp>
	implements LSTNetPredictParams <LSTNetPredictBatchOp> {

	public LSTNetPredictBatchOp() {
		this(new Params());
	}

	public LSTNetPredictBatchOp(Params params) {
		super(LSTNetModelMapper::new, params);
	}
}
