
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToJsonBatchOp extends BaseFormatTransBatchOp<ColumnsToJsonBatchOp>
    implements ColumnsToJsonParams<ColumnsToJsonBatchOp> {

    public ColumnsToJsonBatchOp() {
        this(new Params());
    }

    public ColumnsToJsonBatchOp(Params params) {
        super(FormatType.COLUMNS, FormatType.JSON, params);
    }
}
