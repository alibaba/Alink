package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.pca.PcaModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.feature.PcaPredictParams;

/**
 * pca predict for batch data, it need a pca model which is train from PcaTrainBatchOp
 */
public class PcaPredictBatchOp extends ModelMapBatchOp<PcaPredictBatchOp>
    implements PcaPredictParams<PcaPredictBatchOp> {

    /**
     * default constructor
     */
    public PcaPredictBatchOp() {
        this(null);
    }


    /**
     * constructor.
     *
     * @param params parameter set.
     */
    public PcaPredictBatchOp(Params params) {
        super(PcaModelMapper::new, params);
    }
}
