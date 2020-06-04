
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToKvBatchOp extends BaseFormatTransBatchOp<ColumnsToKvBatchOp>
    implements ColumnsToKvParams<ColumnsToKvBatchOp> {

    public ColumnsToKvBatchOp() {
        this(new Params());
    }

    public ColumnsToKvBatchOp(Params params) {
        super(FormatType.COLUMNS, FormatType.KV, params);
    }
}
