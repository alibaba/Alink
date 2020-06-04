package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class TripleToColumnsBatchOp extends TripleToAnyBatchOp<TripleToColumnsBatchOp>
    implements TripleToColumnsParams<TripleToColumnsBatchOp> {

    public TripleToColumnsBatchOp() {
        this(new Params());
    }

    public TripleToColumnsBatchOp(Params params) {
        super(FormatType.COLUMNS, params.clone());
    }
}
