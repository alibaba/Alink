package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.stream.StreamOperator;

/**
 * StreamOperator to sink stream data to DB.
 */
@IoOpAnnotation(name = "db", ioType = IOType.SinkStream)
public final class DBSinkStreamOp extends BaseSinkStreamOp <DBSinkStreamOp> {

	private BaseDB db;
	private String tableName;
	private TableSchema schema;

	public DBSinkStreamOp(BaseDB db, String tableName) {
		this(db, tableName, null);
	}

	public DBSinkStreamOp(BaseDB db, String tableName, Params parameter) {
		this(db,
			new Params().merge(parameter)
				.set(AnnotationUtils.tableAliasParamKey(db.getClass()), tableName)
		);
	}

	public DBSinkStreamOp(BaseDB db, Params parameter) {
		super(AnnotationUtils.annotatedName(db.getClass()), db.getParams().clone().merge(parameter));

		this.db = db;
		this.tableName = parameter.get(AnnotationUtils.tableAliasParamKey(db.getClass()));
	}

	@Override
	public TableSchema getSchema() {
		return this.schema;
	}

	@Override
	public DBSinkStreamOp sinkFrom(StreamOperator in) {

		Table inTable = in.getOutputTable();
		if (!isAppendStream(inTable)) {
			inTable = DataStreamConversionUtil.toTable(getMLEnvironmentId(), toRetractStream(inTable), in.getSchema());
		}

		//Get table schema
		this.schema = in.getSchema();

		//Sink to DB
		db.sinkStream(this.tableName, inTable, this.getParams(), getMLEnvironmentId());

		return this;
	}
}
