package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.common.io.RedisStringOutputFormat;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.RedisStringSinkParams;

/**
 * StreamOperator to sink data to Redis.
 */
@IoOpAnnotation(name = "redis_string_stream_sink", ioType = IOType.SinkStream)
@ParamSelectColumnSpec(name = "keyCol")
@ParamSelectColumnSpec(name = "valueCol")
@NameCn("导出到Redis")
@NameEn("Redis String Sink")
public final class RedisStringSinkStreamOp extends BaseSinkStreamOp <RedisStringSinkStreamOp>
	implements RedisStringSinkParams <RedisStringSinkStreamOp> {

	public RedisStringSinkStreamOp() {
		this(new Params());
	}

	public RedisStringSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(RedisStringSinkStreamOp.class), params);
	}

	@Override
	public RedisStringSinkStreamOp sinkFrom(StreamOperator <?> in) {
		TableSchema schema = in.getSchema();

		in.getDataStream().writeUsingOutputFormat(
			new RedisStringOutputFormat(getParams(), schema.getFieldNames(), schema.getFieldTypes())
		);

		return this;
	}

}
