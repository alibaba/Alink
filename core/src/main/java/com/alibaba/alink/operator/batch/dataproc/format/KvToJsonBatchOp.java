
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToJsonBatchOp extends BaseFormatTransBatchOp<KvToJsonBatchOp>
    implements KvToJsonParams<KvToJsonBatchOp> {

    public KvToJsonBatchOp() {
        this(new Params());
    }

    public KvToJsonBatchOp(Params params) {
        super(FormatType.KV, FormatType.JSON, params);
    }
}
