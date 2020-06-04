
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToJson extends BaseFormatTrans<VectorToJson> implements VectorToJsonParams<VectorToJson> {

    public VectorToJson() {
        this(new Params());
    }

    public VectorToJson(Params params) {
        super(FormatType.VECTOR, FormatType.JSON, params);
    }
}

