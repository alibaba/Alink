
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToCsvBatchOp extends BaseFormatTransBatchOp<ColumnsToCsvBatchOp>
    implements ColumnsToCsvParams<ColumnsToCsvBatchOp> {

    public ColumnsToCsvBatchOp() {
        this(new Params());
    }

    public ColumnsToCsvBatchOp(Params params) {
        super(FormatType.COLUMNS, FormatType.CSV, params);
    }
}
