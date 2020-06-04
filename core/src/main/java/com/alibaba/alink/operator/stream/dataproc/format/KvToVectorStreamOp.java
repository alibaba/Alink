
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToVectorStreamOp extends BaseFormatTransStreamOp<KvToVectorStreamOp>
    implements KvToVectorParams<KvToVectorStreamOp> {

    public KvToVectorStreamOp() {
        this(new Params());
    }

    public KvToVectorStreamOp(Params params) {
        super(FormatType.KV, FormatType.VECTOR, params);
    }
}
