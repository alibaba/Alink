package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.regression.LassoRegPredictParams;

/**
 * Lasso regression predict batch operator.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("Lasso回归预测")
public final class LassoRegPredictBatchOp extends ModelMapBatchOp <LassoRegPredictBatchOp>
	implements LassoRegPredictParams <LassoRegPredictBatchOp> {

	private static final long serialVersionUID = -8976163339264497718L;

	public LassoRegPredictBatchOp() {
		this(new Params());
	}

	public LassoRegPredictBatchOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
