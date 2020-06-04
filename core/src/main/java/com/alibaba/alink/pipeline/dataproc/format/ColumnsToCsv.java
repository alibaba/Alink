
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToCsv extends BaseFormatTrans<ColumnsToCsv> implements ColumnsToCsvParams<ColumnsToCsv> {

    public ColumnsToCsv() {
        this(new Params());
    }

    public ColumnsToCsv(Params params) {
        super(FormatType.COLUMNS, FormatType.CSV, params);
    }
}

