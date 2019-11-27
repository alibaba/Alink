package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.MySqlDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.MySqlSinkParams;

/**
 * Sink the data to MySql.
 */
@IoOpAnnotation(name = "my_sql_batch_sink", ioType = IOType.SinkBatch)
public final class MySqlSinkBatchOp extends BaseSinkBatchOp <MySqlSinkBatchOp>
	implements MySqlSinkParams <MySqlSinkBatchOp> {

	public MySqlSinkBatchOp() {
		this(new Params());
	}

	public MySqlSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(MySqlDB.class), params);
	}

	@Override
	public MySqlSinkBatchOp sinkFrom(BatchOperator in) {
		try {
			BaseDB db = BaseDB.of(super.getParams());
			DBSinkBatchOp dbSinkBatchOp = new DBSinkBatchOp(db, getOutputTableName(), super.getParams());
			dbSinkBatchOp.linkFrom(in);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return this;
	}
}
