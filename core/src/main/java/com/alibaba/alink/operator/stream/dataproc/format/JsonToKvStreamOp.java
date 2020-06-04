
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToKvStreamOp extends BaseFormatTransStreamOp<JsonToKvStreamOp>
    implements JsonToKvParams<JsonToKvStreamOp> {

    public JsonToKvStreamOp() {
        this(new Params());
    }

    public JsonToKvStreamOp(Params params) {
        super(FormatType.JSON, FormatType.KV, params);
    }
}
