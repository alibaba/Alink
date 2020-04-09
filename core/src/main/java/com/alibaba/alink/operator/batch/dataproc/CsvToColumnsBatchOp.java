package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.StringToColumnsMappers;
import com.alibaba.alink.params.dataproc.CsvToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * CsvToColumnsBatchOp parses a CSV formatted string column to several columns,
 * according to the specified schema.
 */
public final class CsvToColumnsBatchOp extends MapBatchOp<CsvToColumnsBatchOp>
    implements CsvToColumnsParams<CsvToColumnsBatchOp> {

    public CsvToColumnsBatchOp() {
        this(null);
    }

    public CsvToColumnsBatchOp(Params params) {
        super(StringToColumnsMappers.CsvToColumnsMapper::new, params);
    }
}
