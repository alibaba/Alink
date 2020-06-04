
package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToColumnsStreamOp extends BaseFormatTransStreamOp<CsvToColumnsStreamOp>
    implements CsvToColumnsParams<CsvToColumnsStreamOp> {

    public CsvToColumnsStreamOp() {
        this(new Params());
    }

    public CsvToColumnsStreamOp(Params params) {
        super(FormatType.CSV, FormatType.COLUMNS, params);
    }
}
