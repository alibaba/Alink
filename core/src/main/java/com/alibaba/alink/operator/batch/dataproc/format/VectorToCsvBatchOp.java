
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToCsvBatchOp extends BaseFormatTransBatchOp<VectorToCsvBatchOp>
    implements VectorToCsvParams<VectorToCsvBatchOp> {

    public VectorToCsvBatchOp() {
        this(new Params());
    }

    public VectorToCsvBatchOp(Params params) {
        super(FormatType.VECTOR, FormatType.CSV, params);
    }
}
