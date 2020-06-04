
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToVectorBatchOp extends BaseFormatTransBatchOp<KvToVectorBatchOp>
    implements KvToVectorParams<KvToVectorBatchOp> {

    public KvToVectorBatchOp() {
        this(new Params());
    }

    public KvToVectorBatchOp(Params params) {
        super(FormatType.KV, FormatType.VECTOR, params);
    }
}
