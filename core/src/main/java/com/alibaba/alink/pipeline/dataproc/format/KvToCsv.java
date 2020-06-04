
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToCsvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToCsv extends BaseFormatTrans<KvToCsv> implements KvToCsvParams<KvToCsv> {

    public KvToCsv() {
        this(new Params());
    }

    public KvToCsv(Params params) {
        super(FormatType.KV, FormatType.CSV, params);
    }
}

