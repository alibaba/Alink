
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToColumnsStreamOp extends BaseFormatTransStreamOp<KvToColumnsStreamOp>
    implements KvToColumnsParams<KvToColumnsStreamOp> {

    public KvToColumnsStreamOp() {
        this(new Params());
    }

    public KvToColumnsStreamOp(Params params) {
        super(FormatType.KV, FormatType.COLUMNS, params);
    }
}
