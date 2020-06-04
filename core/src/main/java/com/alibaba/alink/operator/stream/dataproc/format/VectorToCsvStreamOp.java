
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToCsvStreamOp extends BaseFormatTransStreamOp<VectorToCsvStreamOp>
    implements VectorToCsvParams<VectorToCsvStreamOp> {

    public VectorToCsvStreamOp() {
        this(new Params());
    }

    public VectorToCsvStreamOp(Params params) {
        super(FormatType.VECTOR, FormatType.CSV, params);
    }
}
