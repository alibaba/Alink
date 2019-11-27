package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.regression.GlmModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.GlmPredictParams;

/**
 * Generalized Linear Model stream predict.
 */
public class GlmPredictStreamOp extends ModelMapStreamOp<GlmPredictStreamOp>
    implements GlmPredictParams<GlmPredictStreamOp> {

    public GlmPredictStreamOp(BatchOperator model) {
        this(model, new Params());
    }

    public GlmPredictStreamOp(BatchOperator model, Params params) {
        super(model, GlmModelMapper::new, params);
    }

}
