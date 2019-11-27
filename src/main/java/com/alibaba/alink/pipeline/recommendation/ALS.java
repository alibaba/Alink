package com.alibaba.alink.pipeline.recommendation;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp;
import com.alibaba.alink.params.recommendation.AlsPredictParams;
import com.alibaba.alink.params.recommendation.AlsTrainParams;
import com.alibaba.alink.pipeline.Trainer;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Matrix factorization using Alternating Least Square method.
 * <p>
 * ALS tries to decompose a matrix R as R = X * Yt. Here X and Y are called factor matrices.
 * Matrix R is usually a sparse matrix representing ratings given from users to items.
 * ALS tries to find X and Y that minimize || R - X * Yt ||^2. This is done by iterations.
 * At each step, X is fixed and Y is solved, then Y is fixed and X is solved.
 * <p>
 * The algorithm is described in "Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007"
 * <p>
 * We also support implicit preference model described in
 * "Collaborative Filtering for Implicit Feedback Datasets, 2008"
 */
public class ALS extends Trainer<ALS, ALSModel> implements
    AlsTrainParams<ALS>,
    AlsPredictParams<ALS> {

    public ALS() {
        super();
    }

    public ALS(Params params) {
        super(params);
    }

    @Override
    protected BatchOperator train(BatchOperator in) {
        return new AlsTrainBatchOp(this.getParams()).linkFrom(in);
    }

}