package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.timeseries.DeepARModelMapper;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.timeseries.DeepARPredictParams;

@NameCn("DeepAR预测")
public class DeepARPredictStreamOp extends ModelMapStreamOp <DeepARPredictStreamOp>
	implements DeepARPredictParams <DeepARPredictStreamOp> {

	public DeepARPredictStreamOp() {
		super(DeepARModelMapper::new, new Params());
	}

	public DeepARPredictStreamOp(Params params) {
		super(DeepARModelMapper::new, params);
	}

	public DeepARPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public DeepARPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, DeepARModelMapper::new, params);
	}
}
