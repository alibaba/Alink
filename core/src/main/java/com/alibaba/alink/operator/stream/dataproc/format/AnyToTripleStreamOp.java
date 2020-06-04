package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.AnyToTripleFlatMapper;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransParams;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.operator.stream.utils.FlatMapStreamOp;
import com.alibaba.alink.params.dataproc.format.ToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

public class AnyToTripleStreamOp<T extends AnyToTripleStreamOp<T>> extends FlatMapStreamOp<T>
    implements ToTripleParams<T> {

    public AnyToTripleStreamOp() {
        this(null);
    }

    public AnyToTripleStreamOp(FormatType formatType, Params params) {
        this(
            (null == params ? new Params() : params)
                .set(FormatTransParams.FROM_FORMAT, formatType)
        );
    }

    public AnyToTripleStreamOp(Params params) {
        super(AnyToTripleFlatMapper::new, params);
    }
}
