
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class ColumnsToJson extends BaseFormatTrans<ColumnsToJson> implements ColumnsToJsonParams<ColumnsToJson> {

    public ColumnsToJson() {
        this(new Params());
    }

    public ColumnsToJson(Params params) {
        super(FormatType.COLUMNS, FormatType.JSON, params);
    }
}

