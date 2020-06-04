
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToKvParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToKvBatchOp extends BaseFormatTransBatchOp<CsvToKvBatchOp>
    implements CsvToKvParams<CsvToKvBatchOp> {

    public CsvToKvBatchOp() {
        this(new Params());
    }

    public CsvToKvBatchOp(Params params) {
        super(FormatType.CSV, FormatType.KV, params);
    }
}
