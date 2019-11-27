package com.alibaba.alink.operator.batch.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.MySqlDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.params.io.MySqlSourceParams;

/**
 * Data source for MySql.
 */
@IoOpAnnotation(name = "my_sql_batch_source", ioType = IOType.SourceBatch)
public final class MySqlSourceBatchOp extends BaseSourceBatchOp <MySqlSourceBatchOp>
	implements MySqlSourceParams <MySqlSourceBatchOp> {

	public MySqlSourceBatchOp() {
		this(new Params());
	}

	public MySqlSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(MySqlDB.class), params);
	}

	@Override
	protected Table initializeDataSource() {
		try {
			BaseDB db = BaseDB.of(super.getParams());
			return new DBSourceBatchOp(db, getInputTableName(), super.getParams()).initializeDataSource();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
