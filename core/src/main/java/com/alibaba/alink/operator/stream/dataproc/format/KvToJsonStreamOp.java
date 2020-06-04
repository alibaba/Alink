
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToJsonStreamOp extends BaseFormatTransStreamOp<KvToJsonStreamOp>
    implements KvToJsonParams<KvToJsonStreamOp> {

    public KvToJsonStreamOp() {
        this(new Params());
    }

    public KvToJsonStreamOp(Params params) {
        super(FormatType.KV, FormatType.JSON, params);
    }
}
