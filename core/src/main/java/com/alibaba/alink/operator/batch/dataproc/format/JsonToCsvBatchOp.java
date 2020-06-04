
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToCsvBatchOp extends BaseFormatTransBatchOp<JsonToCsvBatchOp>
    implements JsonToCsvParams<JsonToCsvBatchOp> {

    public JsonToCsvBatchOp() {
        this(new Params());
    }

    public JsonToCsvBatchOp(Params params) {
        super(FormatType.JSON, FormatType.CSV, params);
    }
}
