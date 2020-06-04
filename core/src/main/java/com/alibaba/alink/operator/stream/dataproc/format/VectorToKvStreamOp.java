
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToKvStreamOp extends BaseFormatTransStreamOp<VectorToKvStreamOp>
    implements VectorToKvParams<VectorToKvStreamOp> {

    public VectorToKvStreamOp() {
        this(new Params());
    }

    public VectorToKvStreamOp(Params params) {
        super(FormatType.VECTOR, FormatType.KV, params);
    }
}
