
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToKv extends BaseFormatTrans<JsonToKv> implements JsonToKvParams<JsonToKv> {

    public JsonToKv() {
        this(new Params());
    }

    public JsonToKv(Params params) {
        super(FormatType.JSON, FormatType.KV, params);
    }
}

