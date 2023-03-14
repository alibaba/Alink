package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.BinningModelMapper;
import com.alibaba.alink.params.finance.BinningPredictParams;

@NameCn("分箱预测")
@NameEn("binning predictor")
public final class BinningPredictBatchOp extends ModelMapBatchOp <BinningPredictBatchOp>
	implements BinningPredictParams <BinningPredictBatchOp> {

	private static final long serialVersionUID = 8864849007288633705L;

	public BinningPredictBatchOp() {
		this(null);
	}

	public BinningPredictBatchOp(Params params) {
		super(BinningModelMapper::new, params);
	}

}
