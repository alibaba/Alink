package com.alibaba.alink.operator.batch.feature;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * EqualWidth discretizer keeps every interval the same width, output the interval
 * as model, and can transform a new data using the model.
 * <p>The output is the index of the interval.
 */
public final class EqualWidthDiscretizerPredictBatchOp extends ModelMapBatchOp <EqualWidthDiscretizerPredictBatchOp>
    implements QuantileDiscretizerPredictParams <EqualWidthDiscretizerPredictBatchOp> {

    public EqualWidthDiscretizerPredictBatchOp() {
        this(null);
    }

    public EqualWidthDiscretizerPredictBatchOp(Params params) {
        super(QuantileDiscretizerModelMapper::new, params);
    }
}
