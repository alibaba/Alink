
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToJsonStreamOp extends BaseFormatTransStreamOp<ColumnsToJsonStreamOp>
    implements ColumnsToJsonParams<ColumnsToJsonStreamOp> {

    public ColumnsToJsonStreamOp() {
        this(new Params());
    }

    public ColumnsToJsonStreamOp(Params params) {
        super(FormatType.COLUMNS, FormatType.JSON, params);
    }
}
