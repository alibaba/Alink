
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToColumnsStreamOp extends BaseFormatTransStreamOp<JsonToColumnsStreamOp>
    implements JsonToColumnsParams<JsonToColumnsStreamOp> {

    public JsonToColumnsStreamOp() {
        this(new Params());
    }

    public JsonToColumnsStreamOp(Params params) {
        super(FormatType.JSON, FormatType.COLUMNS, params);
    }
}
