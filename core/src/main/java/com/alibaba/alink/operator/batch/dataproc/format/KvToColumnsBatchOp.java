
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToColumnsBatchOp extends BaseFormatTransBatchOp<KvToColumnsBatchOp>
    implements KvToColumnsParams<KvToColumnsBatchOp> {

    public KvToColumnsBatchOp() {
        this(new Params());
    }

    public KvToColumnsBatchOp(Params params) {
        super(FormatType.KV, FormatType.COLUMNS, params);
    }
}
