
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToCsvStreamOp extends BaseFormatTransStreamOp<ColumnsToCsvStreamOp>
    implements ColumnsToCsvParams<ColumnsToCsvStreamOp> {

    public ColumnsToCsvStreamOp() {
        this(new Params());
    }

    public ColumnsToCsvStreamOp(Params params) {
        super(FormatType.COLUMNS, FormatType.CSV, params);
    }
}
