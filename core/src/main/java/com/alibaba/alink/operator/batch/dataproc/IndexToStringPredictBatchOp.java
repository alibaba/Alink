package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.IndexToStringModelMapper;
import com.alibaba.alink.params.dataproc.IndexToStringPredictParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Map index to string.
 */
public final class IndexToStringPredictBatchOp
    extends ModelMapBatchOp<IndexToStringPredictBatchOp>
    implements IndexToStringPredictParams<IndexToStringPredictBatchOp> {

    public IndexToStringPredictBatchOp() {
        this(new Params());
    }

    public IndexToStringPredictBatchOp(Params params) {
        super(IndexToStringModelMapper::new, params);
    }
}
