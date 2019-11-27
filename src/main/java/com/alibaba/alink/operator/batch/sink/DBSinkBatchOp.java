package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * The data sink for DB.
 */
@IoOpAnnotation(name = "db", ioType = IOType.SinkBatch)
public final class DBSinkBatchOp extends BaseSinkBatchOp <DBSinkBatchOp> {

	private BaseDB db;
	private String tableName;
	private TableSchema schema = null;

	public DBSinkBatchOp(BaseDB db, String tableName) {
		this(db, tableName, new Params());
	}

	public DBSinkBatchOp(BaseDB db, String tableName, Params parameter) {
		this(db,
			new Params().merge(parameter)
				.set(AnnotationUtils.tableAliasParamKey(db.getClass()), tableName)
		);
	}

	public DBSinkBatchOp(BaseDB db, Params parameter) {
		super(AnnotationUtils.annotatedName(db.getClass()), db.getParams().clone().merge(parameter));

		this.db = db;
		this.tableName = parameter.get(AnnotationUtils.tableAliasParamKey(db.getClass()));
	}

	@Override
	public TableSchema getSchema() {
		return this.schema;
	}

	@Override
	public DBSinkBatchOp sinkFrom(BatchOperator in) {
		//Get table schema
		this.schema = in.getSchema();

		//Sink to DB
		db.sinkBatch(this.tableName, in.getOutputTable(), this.getParams(), in.getMLEnvironmentId());

		return this;
	}
}
