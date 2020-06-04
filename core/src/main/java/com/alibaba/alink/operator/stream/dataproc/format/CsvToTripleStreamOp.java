package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToTripleStreamOp extends AnyToTripleStreamOp<CsvToTripleStreamOp>
    implements CsvToTripleParams<CsvToTripleStreamOp> {
    public CsvToTripleStreamOp() {
        this(new Params());
    }

    public CsvToTripleStreamOp(Params params) {
        super(FormatType.CSV, params);
    }
}
