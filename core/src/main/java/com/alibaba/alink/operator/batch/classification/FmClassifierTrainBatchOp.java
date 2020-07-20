package com.alibaba.alink.operator.batch.classification;
import com.alibaba.alink.operator.common.fm.FmTrainBatchOp;
import com.alibaba.alink.params.recommendation.FmTrainParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * fm classification trainer.
 */
public class FmClassifierTrainBatchOp extends FmTrainBatchOp<FmClassifierTrainBatchOp>
    implements FmTrainParams<FmClassifierTrainBatchOp> {

	private static final long serialVersionUID = -8385944325790904485L;

	public FmClassifierTrainBatchOp() {
        super(new Params(), "binary_classification");
    }
    public FmClassifierTrainBatchOp(Params params) {
        super(params, "binary_classification");
    }
}
