package com.alibaba.alink.operator.common.fm;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.recommendation.FmPredictParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * fm predict batch operator. this operator predict data's label with fm model.
 *
 */
public final class FmPredictBatchOp extends ModelMapBatchOp <FmPredictBatchOp>
    implements FmPredictParams<FmPredictBatchOp> {

	private static final long serialVersionUID = -2512722656868789290L;

	public FmPredictBatchOp() {
        this(new Params());
    }

    public FmPredictBatchOp(Params params) {
        super(FmModelMapper::new, params);
    }

}
