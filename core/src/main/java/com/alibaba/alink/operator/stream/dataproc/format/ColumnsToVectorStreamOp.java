
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToVectorStreamOp extends BaseFormatTransStreamOp<ColumnsToVectorStreamOp>
    implements ColumnsToVectorParams<ColumnsToVectorStreamOp> {

    public ColumnsToVectorStreamOp() {
        this(new Params());
    }

    public ColumnsToVectorStreamOp(Params params) {
        super(FormatType.COLUMNS, FormatType.VECTOR, params);
    }
}
