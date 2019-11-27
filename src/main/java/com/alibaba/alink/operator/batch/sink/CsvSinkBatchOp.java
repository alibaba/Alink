package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.io.CsvSinkParams;

/**
 * Sink to local or HDFS files in CSV format.
 */

@IoOpAnnotation(name = "csv", ioType = IOType.SinkBatch)
public final class CsvSinkBatchOp extends BaseSinkBatchOp<CsvSinkBatchOp>
    implements CsvSinkParams<CsvSinkBatchOp> {

    public CsvSinkBatchOp() {
        this(new Params());
    }

    public CsvSinkBatchOp(Params params) {
        super(AnnotationUtils.annotatedName(CsvSinkBatchOp.class), params);
    }

    public CsvSinkBatchOp(String path) {
        this(new Params().set(FILE_PATH, path).set(OVERWRITE_SINK, true));
    }

    @Override
    public CsvSinkBatchOp sinkFrom(BatchOperator in) {
        final String filePath = getFilePath();
        final String fieldDelim = getFieldDelimiter();
        final int numFiles = getNumFiles();
        final TypeInformation[] types = in.getColTypes();
        final Character quoteChar = getQuoteChar();

        FileSystem.WriteMode mode = FileSystem.WriteMode.NO_OVERWRITE;
        if (getOverwriteSink()) {
            mode = FileSystem.WriteMode.OVERWRITE;
        }

        DataSet<String> textLines = ((DataSet<Row>) in.getDataSet())
            .map(new CsvUtil.FormatCsvFunc(types, fieldDelim, quoteChar))
            .map(new MapFunction<Row, String>() {
                @Override
                public String map(Row value) throws Exception {
                    return (String) value.getField(0);
                }
            });

        textLines.writeAsText(filePath, mode).name("csv_sink").setParallelism(numFiles);
        return this;
    }
}
