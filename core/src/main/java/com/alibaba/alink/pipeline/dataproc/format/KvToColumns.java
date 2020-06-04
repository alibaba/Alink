
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToColumns extends BaseFormatTrans<KvToColumns> implements KvToColumnsParams<KvToColumns> {

    public KvToColumns() {
        this(new Params());
    }

    public KvToColumns(Params params) {
        super(FormatType.KV, FormatType.COLUMNS, params);
    }
}

