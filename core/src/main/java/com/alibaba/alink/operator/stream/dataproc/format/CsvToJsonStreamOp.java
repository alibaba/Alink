
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToJsonStreamOp extends BaseFormatTransStreamOp<CsvToJsonStreamOp>
    implements CsvToJsonParams<CsvToJsonStreamOp> {

    public CsvToJsonStreamOp() {
        this(new Params());
    }

    public CsvToJsonStreamOp(Params params) {
        super(FormatType.CSV, FormatType.JSON, params);
    }
}
