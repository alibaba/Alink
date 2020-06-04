
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToVector extends BaseFormatTrans<KvToVector> implements KvToVectorParams<KvToVector> {

    public KvToVector() {
        this(new Params());
    }

    public KvToVector(Params params) {
        super(FormatType.KV, FormatType.VECTOR, params);
    }
}

