
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToJson extends BaseFormatTrans<CsvToJson> implements CsvToJsonParams<CsvToJson> {

    public CsvToJson() {
        this(new Params());
    }

    public CsvToJson(Params params) {
        super(FormatType.CSV, FormatType.JSON, params);
    }
}

