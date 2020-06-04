package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToTripleBatchOp extends AnyToTripleBatchOp<CsvToTripleBatchOp>
    implements CsvToTripleParams<CsvToTripleBatchOp> {

    private static final long serialVersionUID = 7543648266815893977L;

    public CsvToTripleBatchOp() {
        this(new Params());
    }

    public CsvToTripleBatchOp(Params params) {
        super(FormatType.CSV, params);
    }

}