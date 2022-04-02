package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.classification.LogisticRegressionPredictParams;

/**
 * Logistic regression predict batch operator. this operator predict data's label with linear model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("逻辑回归预测")
public final class LogisticRegressionPredictBatchOp extends ModelMapBatchOp <LogisticRegressionPredictBatchOp>
	implements LogisticRegressionPredictParams <LogisticRegressionPredictBatchOp> {

	private static final long serialVersionUID = 1827377072155195010L;

	public LogisticRegressionPredictBatchOp() {
		this(new Params());
	}

	public LogisticRegressionPredictBatchOp(Params params) {
		super(LinearModelMapper::new, params);
	}

}
