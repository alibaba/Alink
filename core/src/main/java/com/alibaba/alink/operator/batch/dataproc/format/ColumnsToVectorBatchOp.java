
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToVectorBatchOp extends BaseFormatTransBatchOp<ColumnsToVectorBatchOp>
    implements ColumnsToVectorParams<ColumnsToVectorBatchOp> {

    public ColumnsToVectorBatchOp() {
        this(new Params());
    }

    public ColumnsToVectorBatchOp(Params params) {
        super(FormatType.COLUMNS, FormatType.VECTOR, params);
    }
}
