
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToVector extends BaseFormatTrans<JsonToVector> implements JsonToVectorParams<JsonToVector> {

    public JsonToVector() {
        this(new Params());
    }

    public JsonToVector(Params params) {
        super(FormatType.JSON, FormatType.VECTOR, params);
    }
}

