package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatTransMapper;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransParams;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Transform vector to table columns. This transformer will map vector column to columns as designed.
 */
public class BaseFormatTransStreamOp<T extends BaseFormatTransStreamOp<T>> extends MapStreamOp<T> {

    private BaseFormatTransStreamOp() {
        this(null);
    }

    public BaseFormatTransStreamOp(FormatType fromFormat, FormatType toFormat, Params params) {
        this(
            (null == params ? new Params() : params)
                .set(FormatTransParams.FROM_FORMAT, fromFormat)
                .set(FormatTransParams.TO_FORMAT, toFormat)
        );
    }

    private BaseFormatTransStreamOp(Params params) {
        super(FormatTransMapper::new, params);
    }
}
