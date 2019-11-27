package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.LibSvmSinkParams;


/**
 * Sink the data to files in libsvm format.
 */
@IoOpAnnotation(name = "libsvm", ioType = IOType.SinkBatch)
public final class LibSvmSinkBatchOp extends BaseSinkBatchOp<LibSvmSinkBatchOp>
    implements LibSvmSinkParams<LibSvmSinkBatchOp> {

    public LibSvmSinkBatchOp() {
        this(new Params());
    }

    public LibSvmSinkBatchOp(Params params) {
        super(AnnotationUtils.annotatedName(LibSvmSinkBatchOp.class), params);
    }

    public static String formatLibSvm(Object label, Object vector) {
        String labelStr = "";
        if (label != null) {
            labelStr = String.valueOf(label);
        }
        String vectorStr = "";
        if (vector != null) {
            Vector v = VectorUtil.getVector(vector);
            if (v instanceof SparseVector) {
                int[] indices = ((SparseVector) v).getIndices();
                for (int i = 0; i < indices.length; i++) {
                    indices[i] = indices[i] + 1;
                }
            }
            vectorStr = v.toString();
        }
        return labelStr + " " + vectorStr;
    }

    @Override
    public LibSvmSinkBatchOp sinkFrom(BatchOperator in) {
        final String vectorCol = getVectorCol();
        final String labelCol = getLabelCol();

        final int vectorColIdx = TableUtil.findColIndex(in.getColNames(), vectorCol);
        final int labelColIdx = TableUtil.findColIndex(in.getColNames(), labelCol);

        DataSet<Row> outputRows = ((DataSet<Row>) in.getDataSet())
            .map(new MapFunction<Row, Row>() {
                @Override
                public Row map(Row value) throws Exception {
                    return Row.of(formatLibSvm(value.getField(labelColIdx), value.getField(vectorColIdx)));
                }
            });

        BatchOperator outputBatchOp = BatchOperator.fromTable(
            DataSetConversionUtil.toTable(getMLEnvironmentId(), outputRows, new String[]{"f"}, new TypeInformation[]{Types.STRING})
        ).setMLEnvironmentId(getMLEnvironmentId());

        CsvSinkBatchOp sink = new CsvSinkBatchOp()
            .setMLEnvironmentId(getMLEnvironmentId())
            .setFilePath(getFilePath())
            .setOverwriteSink(getOverwriteSink())
            .setFieldDelimiter(" ");

        outputBatchOp.link(sink);
        return this;
    }
}
