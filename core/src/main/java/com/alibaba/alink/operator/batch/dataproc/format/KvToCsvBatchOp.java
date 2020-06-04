
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToCsvBatchOp extends BaseFormatTransBatchOp<KvToCsvBatchOp>
    implements KvToCsvParams<KvToCsvBatchOp> {

    public KvToCsvBatchOp() {
        this(new Params());
    }

    public KvToCsvBatchOp(Params params) {
        super(FormatType.KV, FormatType.CSV, params);
    }
}
