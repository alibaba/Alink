
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToKvBatchOp extends BaseFormatTransBatchOp<JsonToKvBatchOp>
    implements JsonToKvParams<JsonToKvBatchOp> {

    public JsonToKvBatchOp() {
        this(new Params());
    }

    public JsonToKvBatchOp(Params params) {
        super(FormatType.JSON, FormatType.KV, params);
    }
}
