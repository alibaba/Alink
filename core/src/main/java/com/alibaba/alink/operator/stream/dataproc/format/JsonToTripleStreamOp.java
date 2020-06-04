package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToTripleStreamOp extends AnyToTripleStreamOp<JsonToTripleStreamOp>
    implements JsonToTripleParams<JsonToTripleStreamOp> {
    public JsonToTripleStreamOp() {
        this(new Params());
    }

    public JsonToTripleStreamOp(Params params) {
        super(FormatType.JSON, params);
    }
}
