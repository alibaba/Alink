package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfoBatchOp;
import com.alibaba.alink.params.regression.LassoRegTrainParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Train a regression model with L1-regularization.
 */
public final class LassoRegTrainBatchOp extends BaseLinearModelTrainBatchOp<LassoRegTrainBatchOp>
    implements LassoRegTrainParams<LassoRegTrainBatchOp>,
    WithModelInfoBatchOp<LinearRegressorModelInfo, LassoRegTrainBatchOp, LinearRegressorModelInfoBatchOp> {

    private static final long serialVersionUID = 2399798703219983298L;

    public LassoRegTrainBatchOp() {
        this(new Params());
    }

    public LassoRegTrainBatchOp(Params params) {
        super(params.clone(), LinearModelType.LinearReg, "LASSO");
    }

    @Override
    public LinearRegressorModelInfoBatchOp getModelInfoBatchOp() {
        return new LinearRegressorModelInfoBatchOp().linkFrom(this);
    }
}
