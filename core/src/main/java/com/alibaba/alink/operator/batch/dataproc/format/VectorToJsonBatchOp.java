
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToJsonBatchOp extends BaseFormatTransBatchOp<VectorToJsonBatchOp>
    implements VectorToJsonParams<VectorToJsonBatchOp> {

    public VectorToJsonBatchOp() {
        this(new Params());
    }

    public VectorToJsonBatchOp(Params params) {
        super(FormatType.VECTOR, FormatType.JSON, params);
    }
}
