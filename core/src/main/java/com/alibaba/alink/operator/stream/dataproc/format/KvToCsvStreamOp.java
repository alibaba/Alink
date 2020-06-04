
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToCsvStreamOp extends BaseFormatTransStreamOp<KvToCsvStreamOp>
    implements KvToCsvParams<KvToCsvStreamOp> {

    public KvToCsvStreamOp() {
        this(new Params());
    }

    public KvToCsvStreamOp(Params params) {
        super(FormatType.KV, FormatType.CSV, params);
    }
}
