package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToTripleStreamOp extends AnyToTripleStreamOp<KvToTripleStreamOp>
    implements KvToTripleParams<KvToTripleStreamOp> {
    public KvToTripleStreamOp() {
        this(new Params());
    }

    public KvToTripleStreamOp(Params params) {
        super(FormatType.KV, params);
    }
}
