package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToTripleStreamOp extends AnyToTripleStreamOp<ColumnsToTripleStreamOp>
    implements ColumnsToTripleParams<ColumnsToTripleStreamOp> {
    public ColumnsToTripleStreamOp() {
        this(new Params());
    }

    public ColumnsToTripleStreamOp(Params params) {
        super(FormatType.COLUMNS, params);
    }
}
