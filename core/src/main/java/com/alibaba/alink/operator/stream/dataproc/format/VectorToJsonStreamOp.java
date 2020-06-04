
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToJsonStreamOp extends BaseFormatTransStreamOp<VectorToJsonStreamOp>
    implements VectorToJsonParams<VectorToJsonStreamOp> {

    public VectorToJsonStreamOp() {
        this(new Params());
    }

    public VectorToJsonStreamOp(Params params) {
        super(FormatType.VECTOR, FormatType.JSON, params);
    }
}
