package com.alibaba.alink.operator.local.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.common.io.RedisRowOutputFormat;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.io.RedisRowSinkParams;

/**
 * Sink to redis.
 */
@ParamSelectColumnSpec(name = "keyCols")
@ParamSelectColumnSpec(name = "valueCols")
@NameCn("导出到Redis")
@NameEn("Sink Redis")
public final class RedisRowSinkLocalOp extends BaseSinkLocalOp <RedisRowSinkLocalOp>
	implements RedisRowSinkParams <RedisRowSinkLocalOp> {

	public RedisRowSinkLocalOp() {
		this(new Params());
	}

	public RedisRowSinkLocalOp(Params params) {
		super(params);
	}

	@Override
	public RedisRowSinkLocalOp sinkFrom(LocalOperator <?> in) {

		TableSchema schema = in.getSchema();

		output(
			in.getOutputTable().getRows(),
			new RedisRowOutputFormat(getParams(), schema.getFieldNames(), schema.getFieldTypes()),
			1
		);

		return this;
	}

}
