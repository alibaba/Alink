
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToVectorBatchOp extends BaseFormatTransBatchOp<JsonToVectorBatchOp>
    implements JsonToVectorParams<JsonToVectorBatchOp> {

    public JsonToVectorBatchOp() {
        this(new Params());
    }

    public JsonToVectorBatchOp(Params params) {
        super(FormatType.JSON, FormatType.VECTOR, params);
    }
}
