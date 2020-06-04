
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;


public class VectorToColumnsBatchOp extends BaseFormatTransBatchOp<VectorToColumnsBatchOp>
    implements VectorToColumnsParams<VectorToColumnsBatchOp> {

    public VectorToColumnsBatchOp() {
        this(new Params());
    }

    public VectorToColumnsBatchOp(Params params) {
        super(FormatType.VECTOR, FormatType.COLUMNS, params);
    }
}
