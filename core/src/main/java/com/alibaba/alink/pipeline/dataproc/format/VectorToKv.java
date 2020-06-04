
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToKv extends BaseFormatTrans<VectorToKv> implements VectorToKvParams<VectorToKv> {

    public VectorToKv() {
        this(new Params());
    }

    public VectorToKv(Params params) {
        super(FormatType.VECTOR, FormatType.KV, params);
    }
}

