
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToVector extends BaseFormatTrans<ColumnsToVector> implements ColumnsToVectorParams<ColumnsToVector> {

    public ColumnsToVector() {
        this(new Params());
    }

    public ColumnsToVector(Params params) {
        super(FormatType.COLUMNS, FormatType.VECTOR, params);
    }
}

