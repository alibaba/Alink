
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToKv extends BaseFormatTrans<ColumnsToKv> implements ColumnsToKvParams<ColumnsToKv> {

    public ColumnsToKv() {
        this(new Params());
    }

    public ColumnsToKv(Params params) {
        super(FormatType.COLUMNS, FormatType.KV, params);
    }
}

