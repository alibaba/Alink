package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.source.LibSvmSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.LibSvmSourceParams;

/**
 * Stream source that read data in libsvm format.
 */
@IoOpAnnotation(name = "libsvm", ioType = IOType.SourceStream)
public final class LibSvmSourceStreamOp extends BaseSourceStreamOp<LibSvmSourceStreamOp>
    implements LibSvmSourceParams<LibSvmSourceStreamOp> {

    public LibSvmSourceStreamOp() {
        this(new Params());
    }

    public LibSvmSourceStreamOp(Params params) {
        super(AnnotationUtils.annotatedName(LibSvmSourceStreamOp.class), params);
    }

    public LibSvmSourceStreamOp(String filePath) {
        this(new Params()
            .set(FILE_PATH, filePath)
        );
    }

    @Override
    public Table initializeDataSource() {

        StreamOperator source = new CsvSourceStreamOp()
            .setMLEnvironmentId(getMLEnvironmentId())
            .setFilePath(getFilePath())
            .setFieldDelimiter("\n")
            .setSchemaStr("content string");

        DataStream<Row> data = ((DataStream<Row>) source.getDataStream())
            .map(new MapFunction<Row, Row>() {
                @Override
                public Row map(Row value) throws Exception {
                    String line = (String) value.getField(0);
                    Tuple2<Double, Vector> labelAndFeatures = LibSvmSourceBatchOp.parseLibSvmFormat(line);
                    return Row.of(labelAndFeatures.f0, labelAndFeatures.f1);
                }
            });
        return DataStreamConversionUtil.toTable(getMLEnvironmentId(), data, LibSvmSourceBatchOp.LIB_SVM_TABLE_SCHEMA);
    }
}
