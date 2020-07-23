package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.params.feature.QuantileDiscretizerTrainParams;
import com.alibaba.alink.pipeline.Trainer;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * EqualWidth discretizer keeps every interval the same width, output the interval
 * as model, and can transform a new data using the model.
 * <p>The output is the index of the interval.
 */
public class EqualWidthDiscretizer extends Trainer <EqualWidthDiscretizer, QuantileDiscretizerModel>
    implements QuantileDiscretizerTrainParams <EqualWidthDiscretizer>,
    QuantileDiscretizerPredictParams <EqualWidthDiscretizer>, HasLazyPrintModelInfo<EqualWidthDiscretizer> {

    public EqualWidthDiscretizer() {
        super();
    }

    public EqualWidthDiscretizer(Params params) {
        super(params);
    }

    @Override
    protected BatchOperator train(BatchOperator in) {
        return new EqualWidthDiscretizerTrainBatchOp(getParams()).linkFrom(in);
    }
}
