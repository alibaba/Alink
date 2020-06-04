
package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.CsvToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

public class CsvToColumns extends BaseFormatTrans<CsvToColumns> implements CsvToColumnsParams<CsvToColumns> {

    public CsvToColumns() {
        this(new Params());
    }

    public CsvToColumns(Params params) {
        super(FormatType.CSV, FormatType.COLUMNS, params);
    }
}

