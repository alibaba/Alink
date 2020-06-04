
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;

import com.alibaba.alink.params.dataproc.format.VectorToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class VectorToColumnsStreamOp extends BaseFormatTransStreamOp<VectorToColumnsStreamOp>
    implements VectorToColumnsParams<VectorToColumnsStreamOp> {

    public VectorToColumnsStreamOp() {
        this(new Params());
    }

    public VectorToColumnsStreamOp(Params params) {
        super(FormatType.VECTOR, FormatType.COLUMNS, params);
    }

}
