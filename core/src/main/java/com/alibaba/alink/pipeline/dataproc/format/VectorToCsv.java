
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToCsv extends BaseFormatTrans<VectorToCsv> implements VectorToCsvParams<VectorToCsv> {

    public VectorToCsv() {
        this(new Params());
    }

    public VectorToCsv(Params params) {
        super(FormatType.VECTOR, FormatType.CSV, params);
    }
}

