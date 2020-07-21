package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearClassifierModelInfo;
import com.alibaba.alink.operator.common.linear.LinearClassifierModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.params.classification.LinearBinaryClassTrainParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Logistic regression train batch operator. we use log loss func by setting LinearModelType = LR and model
 * name = "Logistic Regression".
 */
public final class LogisticRegressionTrainBatchOp extends BaseLinearModelTrainBatchOp<LogisticRegressionTrainBatchOp>
    implements LinearBinaryClassTrainParams<LogisticRegressionTrainBatchOp>,
    WithModelInfoBatchOp<LinearClassifierModelInfo, LogisticRegressionTrainBatchOp,
        LinearClassifierModelInfoBatchOp> {

    private static final long serialVersionUID = 2337925921060991692L;

    public LogisticRegressionTrainBatchOp() {
        this(new Params());
    }

    public LogisticRegressionTrainBatchOp(Params params) {
        super(params, LinearModelType.LR, "Logistic Regression");
    }

    @Override
    public LinearClassifierModelInfoBatchOp getModelInfoBatchOp() {
        return new LinearClassifierModelInfoBatchOp().linkFrom(this);
    }
}
