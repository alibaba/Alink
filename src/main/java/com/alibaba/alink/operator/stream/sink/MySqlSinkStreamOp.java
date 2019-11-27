package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.MySqlDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.MySqlSinkParams;

/**
 * StreamOperator to sink data to MySql.
 */
@IoOpAnnotation(name = "my_sql_stream_sink", ioType = IOType.SinkStream)
public final class MySqlSinkStreamOp extends BaseSinkStreamOp <MySqlSinkStreamOp>
	implements MySqlSinkParams <MySqlSinkStreamOp> {

	public MySqlSinkStreamOp() {
		this(new Params());
	}

	public MySqlSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(MySqlDB.class), params);
	}

	@Override
	public MySqlSinkStreamOp sinkFrom(StreamOperator in) {
		try {
			BaseDB db = BaseDB.of(super.getParams());
			DBSinkStreamOp dbSinkStreamOp = new DBSinkStreamOp(db, getOutputTableName(), super.getParams());
			dbSinkStreamOp.linkFrom(in);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return this;
	}
}
