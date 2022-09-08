package com.alibaba.alink.operator.local.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.common.io.RedisStringOutputFormat;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.io.RedisStringSinkParams;

/**
 * Sink to redis.
 */
@ParamSelectColumnSpec(name = "keyCols")
@ParamSelectColumnSpec(name = "valueCols")
@NameCn("kv均为String的数据导出到Redis")
@NameEn("Sink string type kv to Redis")
public final class RedisStringSinkLocalOp extends BaseSinkLocalOp <RedisStringSinkLocalOp>
	implements RedisStringSinkParams <RedisStringSinkLocalOp> {

	public RedisStringSinkLocalOp() {
		this(new Params());
	}

	public RedisStringSinkLocalOp(Params params) {
		super(params);
	}

	@Override
	public RedisStringSinkLocalOp sinkFrom(LocalOperator <?> in) {

		TableSchema schema = in.getSchema();

		output(
			in.getOutputTable().getRows(),
			new RedisStringOutputFormat(getParams(), schema.getFieldNames(), schema.getFieldTypes()),
			1
		);

		return this;
	}

}
