
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToKvStreamOp extends BaseFormatTransStreamOp<ColumnsToKvStreamOp>
    implements ColumnsToKvParams<ColumnsToKvStreamOp> {

    public ColumnsToKvStreamOp() {
        this(new Params());
    }

    public ColumnsToKvStreamOp(Params params) {
        super(FormatType.COLUMNS, FormatType.KV, params);
    }
}
