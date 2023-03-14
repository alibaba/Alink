package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.LookupModelMapper;
import com.alibaba.alink.operator.common.timeseries.LSTNetModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.timeseries.LSTNetPredictParams;

@NameCn("LSTNet预测")
@NameEn("LSTNet Prediction")
public class LSTNetPredictStreamOp extends ModelMapStreamOp <LSTNetPredictStreamOp>
	implements LSTNetPredictParams <LSTNetPredictStreamOp> {

	public LSTNetPredictStreamOp() {
		super(LSTNetModelMapper::new, new Params());
	}

	public LSTNetPredictStreamOp(Params params) {
		super(LSTNetModelMapper::new, params);
	}

	public LSTNetPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public LSTNetPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, LSTNetModelMapper::new, params);
	}
}
