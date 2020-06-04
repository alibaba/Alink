package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToTripleBatchOp extends AnyToTripleBatchOp<KvToTripleBatchOp>
    implements KvToTripleParams<KvToTripleBatchOp> {

    public KvToTripleBatchOp() {
        this(new Params());
    }

    public KvToTripleBatchOp(Params params) {
        super(FormatType.KV, params);
    }
}
