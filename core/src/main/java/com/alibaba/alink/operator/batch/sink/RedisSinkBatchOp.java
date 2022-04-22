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
import com.alibaba.alink.operator.common.io.RedisOutputFormat;
import com.alibaba.alink.params.io.RedisSinkParams;

/**
 * Sink to redis.
 */
@IoOpAnnotation(name = "redis", ioType = IOType.SinkBatch)
@ParamSelectColumnSpec(name = "keyCols")
@ParamSelectColumnSpec(name = "valueCols")
@NameCn("导出到Redis")
@NameEn("Sink Redis")
public final class RedisSinkBatchOp extends BaseSinkBatchOp <RedisSinkBatchOp>
	implements RedisSinkParams <RedisSinkBatchOp> {

	public RedisSinkBatchOp() {
		this(new Params());
	}

	public RedisSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(RedisSinkBatchOp.class), params);
	}

	@Override
	public RedisSinkBatchOp sinkFrom(BatchOperator <?> in) {

		TableSchema schema = in.getSchema();

		in.getDataSet().output(
			new RedisOutputFormat(getParams(), schema.getFieldNames(), schema.getFieldTypes())
		);

		return this;
	}

}
