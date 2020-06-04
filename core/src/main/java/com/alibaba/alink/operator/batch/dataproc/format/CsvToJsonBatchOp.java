
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToJsonParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToJsonBatchOp extends BaseFormatTransBatchOp<CsvToJsonBatchOp>
    implements CsvToJsonParams<CsvToJsonBatchOp> {

    public CsvToJsonBatchOp() {
        this(new Params());
    }

    public CsvToJsonBatchOp(Params params) {
        super(FormatType.CSV, FormatType.JSON, params);
    }
}
