
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToKvStreamOp extends BaseFormatTransStreamOp<CsvToKvStreamOp>
    implements CsvToKvParams<CsvToKvStreamOp> {

    public CsvToKvStreamOp() {
        this(new Params());
    }

    public CsvToKvStreamOp(Params params) {
        super(FormatType.CSV, FormatType.KV, params);
    }
}
