package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.common.io.RedisRowOutputFormat;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.RedisRowSinkParams;

/**
 * StreamOperator to sink data to Redis.
 */
@IoOpAnnotation(name = "redis_row_stream_sink", ioType = IOType.SinkStream)
@ParamSelectColumnSpec(name = "keyCols")
@ParamSelectColumnSpec(name = "valueCols")
@NameCn("导出到Redis")
@NameEn("Redis Row Sink")
public final class RedisRowSinkStreamOp extends BaseSinkStreamOp <RedisRowSinkStreamOp>
	implements RedisRowSinkParams <RedisRowSinkStreamOp> {

	public RedisRowSinkStreamOp() {
		this(new Params());
	}

	public RedisRowSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(RedisRowSinkStreamOp.class), params);
	}

	@Override
	public RedisRowSinkStreamOp sinkFrom(StreamOperator <?> in) {
		TableSchema schema = in.getSchema();

		in.getDataStream().writeUsingOutputFormat(
			new RedisRowOutputFormat(getParams(), schema.getFieldNames(), schema.getFieldTypes())
		);

		return this;
	}

}
