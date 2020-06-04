
package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToColumnsBatchOp extends BaseFormatTransBatchOp<CsvToColumnsBatchOp>
    implements CsvToColumnsParams<CsvToColumnsBatchOp> {

    public CsvToColumnsBatchOp() {
        this(new Params());
    }

    public CsvToColumnsBatchOp(Params params) {
        super(FormatType.CSV, FormatType.COLUMNS, params);
    }
}
