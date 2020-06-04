
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToColumnsBatchOp extends BaseFormatTransBatchOp<JsonToColumnsBatchOp>
    implements JsonToColumnsParams<JsonToColumnsBatchOp> {

    public JsonToColumnsBatchOp() {
        this(new Params());
    }

    public JsonToColumnsBatchOp(Params params) {
        super(FormatType.JSON, FormatType.COLUMNS, params);
    }
}
