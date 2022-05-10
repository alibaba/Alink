package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.RedisStringOutputFormat;
import com.alibaba.alink.params.io.RedisStringSinkParams;

/**
 * Sink to redis.
 */
@IoOpAnnotation(name = "redis", ioType = IOType.SinkBatch)
@ParamSelectColumnSpec(name = "keyCols")
@ParamSelectColumnSpec(name = "valueCols")
@NameCn("kv均为String的数据导出到Redis")
@NameEn("Sink string type kv to Redis")
public final class RedisStringSinkBatchOp extends BaseSinkBatchOp <RedisStringSinkBatchOp>
	implements RedisStringSinkParams <RedisStringSinkBatchOp> {

	public RedisStringSinkBatchOp() {
		this(new Params());
	}

	public RedisStringSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(RedisStringSinkBatchOp.class), params);
	}

	@Override
	public RedisStringSinkBatchOp sinkFrom(BatchOperator <?> in) {

		TableSchema schema = in.getSchema();

		in.getDataSet().output(
			new RedisStringOutputFormat(getParams(), schema.getFieldNames(), schema.getFieldTypes())
		);

		return this;
	}

}
