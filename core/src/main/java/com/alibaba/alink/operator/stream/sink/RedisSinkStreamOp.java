package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.common.io.RedisOutputFormat;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.RedisSinkParams;

/**
 * StreamOperator to sink data to Redis.
 */
@IoOpAnnotation(name = "redis_stream_sink", ioType = IOType.SinkStream)
@ParamSelectColumnSpec(name = "keyCols")
@ParamSelectColumnSpec(name = "valueCols")
@NameCn("")
public final class RedisSinkStreamOp extends BaseSinkStreamOp <RedisSinkStreamOp>
	implements RedisSinkParams <RedisSinkStreamOp> {

	public RedisSinkStreamOp() {
		this(new Params());
	}

	public RedisSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(RedisSinkStreamOp.class), params);
	}

	@Override
	public RedisSinkStreamOp sinkFrom(StreamOperator <?> in) {
		TableSchema schema = in.getSchema();

		in.getDataStream().writeUsingOutputFormat(
			new RedisOutputFormat(getParams(), schema.getFieldNames(), schema.getFieldTypes())
		);

		return this;
	}

}
