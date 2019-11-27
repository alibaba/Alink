package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.common.dataproc.StringIndexerModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.dataproc.StringIndexerPredictParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Map string to index.
 */
public final class StringIndexerPredictBatchOp
    extends ModelMapBatchOp<StringIndexerPredictBatchOp>
    implements StringIndexerPredictParams<StringIndexerPredictBatchOp> {

    public StringIndexerPredictBatchOp() {
        this(new Params());
    }

    public StringIndexerPredictBatchOp(Params params) {
        super(StringIndexerModelMapper::new, params);
    }
}
