package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.CsvSinkParams;

/**
 * Sink to local or HDFS files in CSV format.
 */

@IoOpAnnotation(name = "csv", ioType = IOType.SinkStream)
public final class CsvSinkStreamOp extends BaseSinkStreamOp<CsvSinkStreamOp>
    implements CsvSinkParams<CsvSinkStreamOp> {

    private TableSchema schema;

    public CsvSinkStreamOp() {
        this(new Params());
    }

    public CsvSinkStreamOp(String filePath) {
        this(new Params().set(FILE_PATH, filePath));
    }

    public CsvSinkStreamOp(String filePath, String fieldDelimiter) {
        this(new Params().set(FILE_PATH, filePath)
            .set(FIELD_DELIMITER, fieldDelimiter));
    }

    public CsvSinkStreamOp(Params params) {
        super(AnnotationUtils.annotatedName(CsvSinkStreamOp.class), params);
    }

    @Override
    public TableSchema getSchema() {
        return this.schema;
    }

    @Override
    public CsvSinkStreamOp sinkFrom(StreamOperator in) {
        this.schema = in.getSchema();

        final String filePath = getFilePath();
        final String fieldDelim = getFieldDelimiter();
        final String rowDelimiter = getRowDelimiter();
        final int numFiles = getNumFiles();
        final TypeInformation[] types = in.getColTypes();
        final Character quoteChar = getQuoteChar();

        FileSystem.WriteMode writeMode;
        if (getOverwriteSink()) {
            writeMode = FileSystem.WriteMode.OVERWRITE;
        } else {
            writeMode = FileSystem.WriteMode.NO_OVERWRITE;
        }

        DataStream<Row> output = ((DataStream<Row>) in.getDataStream())
            .map(new CsvUtil.FormatCsvFunc(types, fieldDelim, quoteChar))
            .setParallelism(numFiles);

        CsvTableSink cts = new CsvTableSink(filePath, rowDelimiter, numFiles, writeMode);
        cts.emitDataStream(output);
        return this;
    }
}
