package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class TripleToCsvBatchOp extends TripleToAnyBatchOp<TripleToCsvBatchOp>
    implements TripleToCsvParams<TripleToCsvBatchOp> {

    public TripleToCsvBatchOp() {
        this(new Params());
    }

    public TripleToCsvBatchOp(Params params) {
        super(FormatType.CSV, params.clone());
    }
}
