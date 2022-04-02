package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.NameCn;
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
@NameCn("LibSvm文件导出")
public final class LibSvmSinkStreamOp extends BaseSinkStreamOp <LibSvmSinkStreamOp>
	implements LibSvmSinkParams <LibSvmSinkStreamOp> {

	private static final long serialVersionUID = -8838742868638097880L;

	public LibSvmSinkStreamOp() {
		this(new Params());
	}

	public LibSvmSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(LibSvmSinkStreamOp.class), params);
	}

	@Override
	public LibSvmSinkStreamOp sinkFrom(StreamOperator<?> in) {
		final String vectorCol = getVectorCol();
		final String labelCol = getLabelCol();

		final int vectorColIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorCol);
		final int labelColIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), labelCol);
		final int startIndex = getParams().get(LibSvmSinkParams.START_INDEX);

		DataStream <Row> outputRows = in.getDataStream()
			.map(new MapFunction <Row, Row>() {
				private static final long serialVersionUID = 8548456443314432148L;

				@Override
				public Row map(Row value) throws Exception {
					return Row.of(LibSvmSinkBatchOp
						.formatLibSvm(value.getField(labelColIdx), value.getField(vectorColIdx), startIndex));
				}
			});

		StreamOperator<?> outputStreamOp = StreamOperator.fromTable(
			DataStreamConversionUtil
				.toTable(getMLEnvironmentId(), outputRows, new String[] {"f"}, new TypeInformation[] {Types.STRING})
		).setMLEnvironmentId(getMLEnvironmentId());

		CsvSinkStreamOp sink = new CsvSinkStreamOp()
			.setMLEnvironmentId(getMLEnvironmentId())
			.setFilePath(getFilePath())
			.setQuoteChar(null)
			.setOverwriteSink(getOverwriteSink())
			.setFieldDelimiter(" ");

		outputStreamOp.link(sink);
		return this;
	}
}
