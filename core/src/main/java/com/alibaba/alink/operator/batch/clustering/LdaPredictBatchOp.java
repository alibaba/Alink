package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.clustering.LdaModelMapper;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.clustering.LdaPredictParams;

/**
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 */
public final class LdaPredictBatchOp extends ModelMapBatchOp<LdaPredictBatchOp>
        implements LdaPredictParams<LdaPredictBatchOp> {

    /**
     * Constructor.
     */
    public LdaPredictBatchOp() {
        this(null);
    }

    /**
     * Constructor with the algorithm params.
     */
    public LdaPredictBatchOp(Params params) {
        super(LdaModelMapper::new, params);
    }

}
