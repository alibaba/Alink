package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToTripleStreamOp extends AnyToTripleStreamOp<VectorToTripleStreamOp>
    implements VectorToTripleParams<VectorToTripleStreamOp> {
    public VectorToTripleStreamOp() {
        this(new Params());
    }

    public VectorToTripleStreamOp(Params params) {
        super(FormatType.VECTOR, params);
    }
}
