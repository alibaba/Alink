package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class TripleToJsonBatchOp extends TripleToAnyBatchOp<TripleToJsonBatchOp>
    implements TripleToJsonParams<TripleToJsonBatchOp> {

    public TripleToJsonBatchOp() {
        this(new Params());
    }

    public TripleToJsonBatchOp(Params params) {
        super(FormatType.JSON, params.clone());
    }
}
