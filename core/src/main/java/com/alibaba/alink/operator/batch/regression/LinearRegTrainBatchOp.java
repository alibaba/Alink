package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfoBatchOp;
import com.alibaba.alink.params.regression.LinearRegTrainParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Train a regression model.
 */
public final class LinearRegTrainBatchOp extends BaseLinearModelTrainBatchOp<LinearRegTrainBatchOp>
    implements LinearRegTrainParams<LinearRegTrainBatchOp>,
    WithModelInfoBatchOp<LinearRegressorModelInfo, LinearRegTrainBatchOp, LinearRegressorModelInfoBatchOp> {

    private static final long serialVersionUID = -8737435600011807472L;

    public LinearRegTrainBatchOp() {
        this(new Params());
    }

    public LinearRegTrainBatchOp(Params params) {
        super(params.clone(), LinearModelType.LinearReg, "Linear Regression");
    }

    @Override
    public LinearRegressorModelInfoBatchOp getModelInfoBatchOp() {
        return new LinearRegressorModelInfoBatchOp().linkFrom(this);
    }
}
