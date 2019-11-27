package com.alibaba.alink.operator.batch.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;

/**
 * A data source for DB.
 */
@IoOpAnnotation(name = "db", ioType = IOType.SourceBatch)
public final class DBSourceBatchOp extends BaseSourceBatchOp <DBSourceBatchOp> {
	private BaseDB db;

	public DBSourceBatchOp(BaseDB db, String tableName) throws Exception {
		this(db, tableName, new Params());
	}

	public DBSourceBatchOp(BaseDB db, String tableName, Params parameter) throws Exception {
		this(db,
			new Params().merge(parameter)
				.set(AnnotationUtils.tableAliasParamKey(db.getClass()), tableName)
		);
	}

	public DBSourceBatchOp(BaseDB db, Params parameter) throws Exception {
		super(AnnotationUtils.annotatedName(db.getClass()), db.getParams().clone().merge(parameter));
		this.db = db;
	}

	@Override
	public Table initializeDataSource() {
		String tableName = this.getParams().get(AnnotationUtils.tableAliasParamKey(db.getClass()));
		try {
			return db.getBatchTable(tableName, this.getParams(), getMLEnvironmentId());
		} catch (Exception e) {
			throw new RuntimeException("Fail to get table from db: " + e);
		}
	}
}
