package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.classification.LinearModelMapperParams;

/**
 */
@NameCn("Ftrl在线预测")
@NameEn("Follow the regularized leader model prediction")
public final class FtrlPredictStreamOp extends BaseOnlinePredictStreamOp <FtrlPredictStreamOp>
	implements LinearModelMapperParams <FtrlPredictStreamOp> {

	public FtrlPredictStreamOp(BatchOperator<?> model) {
		this(model, new Params());
	}

	public FtrlPredictStreamOp(BatchOperator<?> model, Params params) {
		super(model, LinearModelMapper::new, params);
	}
}
