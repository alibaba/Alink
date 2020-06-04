
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToVectorStreamOp extends BaseFormatTransStreamOp<CsvToVectorStreamOp>
    implements CsvToVectorParams<CsvToVectorStreamOp> {

    public CsvToVectorStreamOp() {
        this(new Params());
    }

    public CsvToVectorStreamOp(Params params) {
        super(FormatType.CSV, FormatType.VECTOR, params);
    }
}
