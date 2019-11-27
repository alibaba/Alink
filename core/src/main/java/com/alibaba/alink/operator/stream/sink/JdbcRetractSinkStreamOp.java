package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.JdbcDB;
import com.alibaba.alink.operator.common.io.jdbc.JDBCUpserOutputFormat;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.VectorSerializeStreamOp;

/**
 * StreamOperator to sink retract stream to JDBC.
 */
public final class JdbcRetractSinkStreamOp extends StreamOperator<JdbcRetractSinkStreamOp> {
	private JdbcDB jdbcDB;
	private String tableName;
	private String[] primaryColNames;

	public JdbcRetractSinkStreamOp(JdbcDB jdbcDB, String tableName, String[] primaryColNames) {
		super(null);
		this.jdbcDB = jdbcDB;
		this.tableName = tableName;
		this.primaryColNames = primaryColNames;

	}

	@Override
	public JdbcRetractSinkStreamOp linkFrom(StreamOperator<?>... inputs) {
		StreamOperator<?> in = checkAndGetFirst(inputs);
		in = in.link(new VectorSerializeStreamOp().setMLEnvironmentId(getMLEnvironmentId()));
		if (this.primaryColNames == null || this.primaryColNames.length == 0) {
			throw new RuntimeException("primary key must not be empty.");
		}
		try {
			if (!jdbcDB.hasTable(tableName)) {
				jdbcDB.createTable(tableName, in.getSchema());
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}

		DataStream <Tuple2 <Boolean, Row>> tuple2Stream =
			MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment().toRetractStream(in.getOutputTable(), Row.class);

		tuple2Stream.writeUsingOutputFormat(
			new JDBCUpserOutputFormat(jdbcDB.getUserName(),
				jdbcDB.getPassword(),
				jdbcDB.getDriverName(),
				jdbcDB.getDbUrl(),
				tableName, primaryColNames));

		return this;
	}
}
