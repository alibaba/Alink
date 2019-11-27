package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.PcaTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.feature.PcaPredictParams;
import com.alibaba.alink.params.feature.PcaTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * PCA is dimension reduction of discrete feature, projects vectors to a low-dimensional space.
 * PcaTrainBatchOp is train a model which can be used to batch predict and stream predict
 * The calculation is done using eigen on the correlation or covariance matrix.
 */
public class PCA extends Trainer<PCA, PCAModel> implements
    PcaTrainParams<PCA>,
    PcaPredictParams<PCA> {

    public PCA() {
        super();
    }

    public PCA(Params params) {
        super(params);
    }

    @Override
    protected BatchOperator train(BatchOperator in) {
        return new PcaTrainBatchOp(this.getParams()).linkFrom(in);
    }
}
