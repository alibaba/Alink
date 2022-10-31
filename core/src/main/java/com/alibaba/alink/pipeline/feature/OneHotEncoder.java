package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.OneHotTrainBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.feature.OneHotTrainLocalOp;
import com.alibaba.alink.params.feature.OneHotPredictParams;
import com.alibaba.alink.params.feature.OneHotTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * One hot pipeline op.
 */
@NameCn("独热编码")
public class OneHotEncoder extends Trainer <OneHotEncoder, OneHotEncoderModel> implements
	OneHotTrainParams <OneHotEncoder>,
	OneHotPredictParams <OneHotEncoder>,
	HasLazyPrintModelInfo <OneHotEncoder> {

	private static final long serialVersionUID = -987476648422234074L;

	public OneHotEncoder() {
		super();
	}

	public OneHotEncoder(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new OneHotTrainBatchOp(this.getParams()).linkFrom(in);
	}

	@Override
	protected LocalOperator <?> train(LocalOperator <?> in) {
		return new OneHotTrainLocalOp(this.getParams()).linkFrom(in);
	}
}
