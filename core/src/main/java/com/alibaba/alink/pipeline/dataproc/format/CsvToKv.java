
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToKv extends BaseFormatTrans<CsvToKv> implements CsvToKvParams<CsvToKv> {

    public CsvToKv() {
        this(new Params());
    }

    public CsvToKv(Params params) {
        super(FormatType.CSV, FormatType.KV, params);
    }
}

