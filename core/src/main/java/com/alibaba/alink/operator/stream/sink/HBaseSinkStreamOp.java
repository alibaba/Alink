package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.common.io.HBaseOutputFormat;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.HBaseSinkParams;

/**
 * StreamOperator to sink data to HBase.
 */
@IoOpAnnotation(name = "hbase_stream_sink", ioType = IOType.SinkStream)
@ParamSelectColumnSpec(name = "rowKeyCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("导出到HBase")
public final class HBaseSinkStreamOp extends BaseSinkStreamOp <HBaseSinkStreamOp>
	implements HBaseSinkParams <HBaseSinkStreamOp> {

	public HBaseSinkStreamOp() {
		this(new Params());
	}

	public HBaseSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(HBaseSinkStreamOp.class), params);
	}

	@Override
	public HBaseSinkStreamOp sinkFrom(StreamOperator <?> in) {
		TableSchema schema = in.getSchema();

		in.getDataStream().writeUsingOutputFormat(
			new HBaseOutputFormat(getParams(), schema.getFieldNames(), schema.getFieldTypes())
		);

		return this;
	}

}
