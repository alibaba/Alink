
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToKvBatchOp extends BaseFormatTransBatchOp<VectorToKvBatchOp>
    implements VectorToKvParams<VectorToKvBatchOp> {

    public VectorToKvBatchOp() {
        this(new Params());
    }

    public VectorToKvBatchOp(Params params) {
        super(FormatType.VECTOR, FormatType.KV, params);
    }
}
