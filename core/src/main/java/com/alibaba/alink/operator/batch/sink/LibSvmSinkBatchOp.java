package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.LibSvmSinkBatchParams;
import com.alibaba.alink.params.io.LibSvmSinkParams;

import static com.alibaba.alink.operator.local.sink.LibSvmSinkLocalOp.formatLibSvm;

/**
 * Sink the data to files in libsvm format.
 */
@IoOpAnnotation(name = "libsvm", ioType = IOType.SinkBatch)
@NameCn("LibSvm文件导出")
public final class LibSvmSinkBatchOp extends BaseSinkBatchOp <LibSvmSinkBatchOp>
	implements LibSvmSinkBatchParams <LibSvmSinkBatchOp> {

	private static final long serialVersionUID = 1706349265088035032L;

	public LibSvmSinkBatchOp() {
		this(new Params());
	}

	public LibSvmSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(LibSvmSinkBatchOp.class), params);
	}

	private static SparseVector toSparseVector(DenseVector v) {
		int[] indices = new int[v.size()];
		double[] values = v.getData();
		for (int i = 0; i < indices.length; i++) {
			indices[i] = i;
		}
		return new SparseVector(-1, indices, values);
	}

	@Override
	public LibSvmSinkBatchOp sinkFrom(BatchOperator <?> in) {
		final String vectorCol = getVectorCol();
		final String labelCol = getLabelCol();

		final int vectorColIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorCol);
		final int labelColIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), labelCol);
		final int startIndex = getParams().get(LibSvmSinkParams.START_INDEX);

		DataSet <Row> outputRows = in.getDataSet()
			.map(new MapFunction <Row, Row>() {
				private static final long serialVersionUID = 8796282303884042197L;

				@Override
				public Row map(Row value) {
					return Row.of(
						formatLibSvm(value.getField(labelColIdx), value.getField(vectorColIdx), startIndex)
					);
				}
			});

		BatchOperator <?> outputBatchOp = BatchOperator.fromTable(
			DataSetConversionUtil.toTable(
				getMLEnvironmentId(), outputRows, new String[] {"f"}, new TypeInformation[] {Types.STRING}
			)
		).setMLEnvironmentId(getMLEnvironmentId());

		CsvSinkBatchOp sink = new CsvSinkBatchOp()
			.setMLEnvironmentId(getMLEnvironmentId())
			.setQuoteChar(null)
			.setFilePath(getFilePath())
			.setOverwriteSink(getOverwriteSink())
			.setFieldDelimiter(" ")
			.setPartitionCols(getPartitionCols());

		outputBatchOp.link(sink);
		return this;
	}
}
