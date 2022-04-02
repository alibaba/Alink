package com.alibaba.alink.operator.stream.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.common.tensorflow.TFSavedModelPredictMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.tensorflow.savedmodel.TFSavedModelPredictParams;

/**
 * This operator loads a tensorflow SavedModel, and performs prediction with Tensorflow's Java sdk.
 */
@ParamSelectColumnSpec(name = "selectedCols")
@NameCn("TF SavedModel模型预测")
public final class TFSavedModelPredictStreamOp extends MapStreamOp<TFSavedModelPredictStreamOp>
    implements TFSavedModelPredictParams<TFSavedModelPredictStreamOp> {

    public TFSavedModelPredictStreamOp() {
        this(new Params());
    }

    public TFSavedModelPredictStreamOp(Params params) {
        super(TFSavedModelPredictMapper::new, params);
    }
}
