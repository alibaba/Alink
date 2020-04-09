package com.alibaba.alink.operator.stream.dataproc;

import com.alibaba.alink.operator.common.dataproc.StringToColumnsMappers;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.CsvToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * CsvToColumnsStreamOp parses a CSV formatted string column to several columns,
 * according to the specified schema.
 */
public final class CsvToColumnsStreamOp extends MapStreamOp<CsvToColumnsStreamOp>
    implements CsvToColumnsParams<CsvToColumnsStreamOp> {

    public CsvToColumnsStreamOp() {
        this(null);
    }

    public CsvToColumnsStreamOp(Params params) {
        super(StringToColumnsMappers.CsvToColumnsMapper::new, params);
    }

}
