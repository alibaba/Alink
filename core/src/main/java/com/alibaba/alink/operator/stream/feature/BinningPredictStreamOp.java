package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.BinningModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.finance.BinningPredictParams;

@NameCn("分箱预测")
@NameEn("Binning Prediction")
public class BinningPredictStreamOp extends ModelMapStreamOp <BinningPredictStreamOp>
	implements BinningPredictParams <BinningPredictStreamOp> {

	private static final long serialVersionUID = -521696011368917163L;

	public BinningPredictStreamOp() {
		super(BinningModelMapper::new, new Params());
	}

	public BinningPredictStreamOp(Params params) {
		super(BinningModelMapper::new, params);
	}

	public BinningPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public BinningPredictStreamOp(BatchOperator model, Params params) {
		super(model, BinningModelMapper::new, params);
	}

}
