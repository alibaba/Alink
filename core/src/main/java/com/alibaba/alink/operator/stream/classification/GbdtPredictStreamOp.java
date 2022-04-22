package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.GbdtPredictParams;

/**
 * The stream operator that predict the data using the binary gbdt model.
 */
@NameCn("GBDT分类器预测")
public class GbdtPredictStreamOp extends ModelMapStreamOp <GbdtPredictStreamOp>
	implements GbdtPredictParams <GbdtPredictStreamOp> {
	private static final long serialVersionUID = 7921961518801253990L;

	public GbdtPredictStreamOp() {
		super(GbdtModelMapper::new, new Params());
	}

	public GbdtPredictStreamOp(Params params) {
		super(GbdtModelMapper::new, params);
	}

	public GbdtPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public GbdtPredictStreamOp(BatchOperator model, Params params) {
		super(model, GbdtModelMapper::new, params);
	}
}

