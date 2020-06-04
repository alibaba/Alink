
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToCsvStreamOp extends BaseFormatTransStreamOp<JsonToCsvStreamOp>
    implements JsonToCsvParams<JsonToCsvStreamOp> {

    public JsonToCsvStreamOp() {
        this(new Params());
    }

    public JsonToCsvStreamOp(Params params) {
        super(FormatType.JSON, FormatType.CSV, params);
    }
}
