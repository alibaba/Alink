package com.alibaba.alink.operator.stream.dataproc;

import com.alibaba.alink.operator.common.dataproc.StringIndexerModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.StringIndexerPredictParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Map string to index.
 */
public final class StringIndexerPredictStreamOp
    extends ModelMapStreamOp<StringIndexerPredictStreamOp>
    implements StringIndexerPredictParams<StringIndexerPredictStreamOp> {

    public StringIndexerPredictStreamOp(BatchOperator model) {
        this(model, new Params());
    }

    public StringIndexerPredictStreamOp(BatchOperator model, Params params) {
        super(model, StringIndexerModelMapper::new, params);
    }
}
