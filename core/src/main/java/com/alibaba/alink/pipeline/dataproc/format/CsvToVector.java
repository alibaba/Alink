
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToVector extends BaseFormatTrans<CsvToVector> implements CsvToVectorParams<CsvToVector> {

    public CsvToVector() {
        this(new Params());
    }

    public CsvToVector(Params params) {
        super(FormatType.CSV, FormatType.VECTOR, params);
    }
}

