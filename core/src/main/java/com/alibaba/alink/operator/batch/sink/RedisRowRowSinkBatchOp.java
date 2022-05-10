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
import com.alibaba.alink.operator.common.io.RedisRowOutputFormat;
import com.alibaba.alink.params.io.RedisRowSinkParams;

/**
 * Sink to redis.
 */
@IoOpAnnotation(name = "redis", ioType = IOType.SinkBatch)
@ParamSelectColumnSpec(name = "keyCols")
@ParamSelectColumnSpec(name = "valueCols")
@NameCn("导出到Redis")
@NameEn("Sink Redis")
public final class RedisRowRowSinkBatchOp extends BaseSinkBatchOp <RedisRowRowSinkBatchOp>
	implements RedisRowSinkParams <RedisRowRowSinkBatchOp> {

	public RedisRowRowSinkBatchOp() {
		this(new Params());
	}

	public RedisRowRowSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(RedisRowRowSinkBatchOp.class), params);
	}

	@Override
	public RedisRowRowSinkBatchOp sinkFrom(BatchOperator <?> in) {

		TableSchema schema = in.getSchema();

		in.getDataSet().output(
			new RedisRowOutputFormat(getParams(), schema.getFieldNames(), schema.getFieldTypes())
		);

		return this;
	}

}
