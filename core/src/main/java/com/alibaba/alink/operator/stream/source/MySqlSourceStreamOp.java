package com.alibaba.alink.operator.stream.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.MySqlDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.params.io.MySqlSourceParams;

/**
 * Stream source that reads data from MySql.
 */
@IoOpAnnotation(name = "my_sql_stream_source", ioType = IOType.SourceStream)
public final class MySqlSourceStreamOp extends BaseSourceStreamOp <MySqlSourceStreamOp>
	implements MySqlSourceParams <MySqlSourceStreamOp> {

	public MySqlSourceStreamOp() {
		this(new Params());
	}

	public MySqlSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(MySqlDB.class), params);
	}

	@Override
	protected Table initializeDataSource() {
		try {
			BaseDB db = BaseDB.of(super.getParams());
			return new DBSourceStreamOp(db, getInputTableName(), super.getParams()).initializeDataSource();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
