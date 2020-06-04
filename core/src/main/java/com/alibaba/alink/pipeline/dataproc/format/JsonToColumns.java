
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToColumns extends BaseFormatTrans<JsonToColumns> implements JsonToColumnsParams<JsonToColumns> {

    public JsonToColumns() {
        this(new Params());
    }

    public JsonToColumns(Params params) {
        super(FormatType.JSON, FormatType.COLUMNS, params);
    }
}

