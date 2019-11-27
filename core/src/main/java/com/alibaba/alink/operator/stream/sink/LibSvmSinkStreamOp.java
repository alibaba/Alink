package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.sink.LibSvmSinkBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.LibSvmSinkParams;

/**
 * StreamOperator to sink data in libsvm format.
 */
@IoOpAnnotation(name = "libsvm", ioType = IOType.SinkStream)
public final class LibSvmSinkStreamOp extends BaseSinkStreamOp<LibSvmSinkStreamOp>
    implements LibSvmSinkParams<LibSvmSinkStreamOp> {

    public LibSvmSinkStreamOp() {
        this(new Params());
    }

    public LibSvmSinkStreamOp(Params params) {
        super(AnnotationUtils.annotatedName(LibSvmSinkStreamOp.class), params);
    }

    @Override
    public LibSvmSinkStreamOp sinkFrom(StreamOperator in) {
        final String vectorCol = getVectorCol();
        final String labelCol = getLabelCol();

        final int vectorColIdx = TableUtil.findColIndex(in.getColNames(), vectorCol);
        final int labelColIdx = TableUtil.findColIndex(in.getColNames(), labelCol);

        DataStream<Row> outputRows = ((DataStream<Row>) in.getDataStream())
            .map(new MapFunction<Row, Row>() {
                @Override
                public Row map(Row value) throws Exception {
                    return Row.of(LibSvmSinkBatchOp.formatLibSvm(value.getField(labelColIdx), value.getField(vectorColIdx)));
                }
            });

        StreamOperator outputStreamOp = StreamOperator.fromTable(
            DataStreamConversionUtil.toTable(getMLEnvironmentId(), outputRows, new String[]{"f"}, new TypeInformation[]{Types.STRING})
        ).setMLEnvironmentId(getMLEnvironmentId());

        CsvSinkStreamOp sink = new CsvSinkStreamOp()
            .setMLEnvironmentId(getMLEnvironmentId())
            .setFilePath(getFilePath())
            .setOverwriteSink(getOverwriteSink())
            .setFieldDelimiter(" ");

        outputStreamOp.link(sink);
        return this;
    }
}
