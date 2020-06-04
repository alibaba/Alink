
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToVectorBatchOp extends BaseFormatTransBatchOp<CsvToVectorBatchOp>
    implements CsvToVectorParams<CsvToVectorBatchOp> {

    public CsvToVectorBatchOp() {
        this(new Params());
    }

    public CsvToVectorBatchOp(Params params) {
        super(FormatType.CSV, FormatType.VECTOR, params);
    }
}
