package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class TripleToKvBatchOp extends TripleToAnyBatchOp<TripleToKvBatchOp>
    implements TripleToKvParams<TripleToKvBatchOp> {

    public TripleToKvBatchOp() {
        this(null);
    }

    public TripleToKvBatchOp(Params params) {
        super(FormatType.KV, params);
    }
}
