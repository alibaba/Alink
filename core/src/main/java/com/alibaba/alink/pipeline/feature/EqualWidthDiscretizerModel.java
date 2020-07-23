package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.pipeline.MapModel;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * EqualWidth discretizer keeps every interval the same width, output the interval
 * as model, and can transform a new data using the model.
 * <p>The output is the index of the interval.
 */
public class EqualWidthDiscretizerModel extends MapModel<EqualWidthDiscretizerModel>
    implements QuantileDiscretizerPredictParams <EqualWidthDiscretizerModel> {

    public EqualWidthDiscretizerModel(){
        this(null);
    }

    public EqualWidthDiscretizerModel(Params params) {
        super(QuantileDiscretizerModelMapper::new, params);
    }
}
