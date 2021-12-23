package com.alibaba.alink.operator.stream.tensorflow;

import com.alibaba.alink.operator.common.tensorflow.TFSavedModelPredictMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.tensorflow.savedmodel.TFSavedModelPredictParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * This operator loads a tensorflow SavedModel, and performs prediction with Tensorflow's Java sdk.
 */
public final class TFSavedModelPredictStreamOp extends MapStreamOp<TFSavedModelPredictStreamOp>
    implements TFSavedModelPredictParams<TFSavedModelPredictStreamOp> {

    public TFSavedModelPredictStreamOp() {
        this(new Params());
    }

    public TFSavedModelPredictStreamOp(Params params) {
        super(TFSavedModelPredictMapper::new, params);
    }
}
