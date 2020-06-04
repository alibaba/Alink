
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToCsv extends BaseFormatTrans<JsonToCsv> implements JsonToCsvParams<JsonToCsv> {

    public JsonToCsv() {
        this(new Params());
    }

    public JsonToCsv(Params params) {
        super(FormatType.JSON, FormatType.CSV, params);
    }
}

