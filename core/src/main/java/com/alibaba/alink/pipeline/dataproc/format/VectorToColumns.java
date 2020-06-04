
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToColumns extends BaseFormatTrans<VectorToColumns> implements VectorToColumnsParams<VectorToColumns> {

    public VectorToColumns() {
        this(new Params());
    }

    public VectorToColumns(Params params) {
        super(FormatType.VECTOR, FormatType.COLUMNS, params);
    }
}

