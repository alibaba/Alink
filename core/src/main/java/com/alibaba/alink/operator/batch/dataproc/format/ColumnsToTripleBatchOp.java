package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToTripleBatchOp extends AnyToTripleBatchOp<ColumnsToTripleBatchOp>
    implements ColumnsToTripleParams<ColumnsToTripleBatchOp> {

    private static final long serialVersionUID = 7543648266815893977L;

    public ColumnsToTripleBatchOp() {
        this(new Params());
    }

    public ColumnsToTripleBatchOp(Params params) {
        super(FormatType.COLUMNS, params);
    }

}