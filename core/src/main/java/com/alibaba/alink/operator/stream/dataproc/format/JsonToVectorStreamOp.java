
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToVectorStreamOp extends BaseFormatTransStreamOp<JsonToVectorStreamOp>
    implements JsonToVectorParams<JsonToVectorStreamOp> {

    public JsonToVectorStreamOp() {
        this(new Params());
    }

    public JsonToVectorStreamOp(Params params) {
        super(FormatType.JSON, FormatType.VECTOR, params);
    }
}
