
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class KvToJson extends BaseFormatTrans<KvToJson> implements KvToJsonParams<KvToJson> {

    public KvToJson() {
        this(new Params());
    }

    public KvToJson(Params params) {
        super(FormatType.KV, FormatType.JSON, params);
    }
}

