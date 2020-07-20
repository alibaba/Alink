package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.operator.common.fm.FmTrainBatchOp;
import com.alibaba.alink.params.recommendation.FmTrainParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Fm regression trainer.
 */
public class FmRegressorTrainBatchOp extends FmTrainBatchOp<FmRegressorTrainBatchOp>
    implements FmTrainParams<FmRegressorTrainBatchOp> {
	private static final long serialVersionUID = 8297633489045835451L;

	public FmRegressorTrainBatchOp(Params params) {
        super(params, "regression");
    }

    public FmRegressorTrainBatchOp() {
        super(new Params(), "regression");
    }
}