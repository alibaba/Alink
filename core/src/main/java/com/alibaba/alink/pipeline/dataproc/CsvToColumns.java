package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.common.dataproc.StringToColumnsMappers;
import com.alibaba.alink.params.dataproc.CsvToColumnsParams;
import com.alibaba.alink.pipeline.MapTransformer;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * CsvToColumns parses a CSV formatted string column to several columns,
 * according to the specified schema.
 */
public class CsvToColumns extends MapTransformer<CsvToColumns>
    implements CsvToColumnsParams<CsvToColumns> {

    public CsvToColumns() {
        this(null);
    }

    public CsvToColumns(Params params) {
        super(StringToColumnsMappers.CsvToColumnsMapper::new, params);
    }

}
