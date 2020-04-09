package com.alibaba.alink.common.io;

import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.DBAnnotation;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.io.MySqlDBParams;
import com.alibaba.alink.params.io.MySqlSourceParams;

/**
 * DB of MySql.
 */
@DBAnnotation(name = "mysql")
public class MySqlDB extends JdbcDB {

	/**
	 * Name mysql database.
	 */
	private String dbName;

	/**
	 * Ip of mysql service.
	 */
	private String ip;

	/**
	 * Port of mysql service.
	 */
	private String port;

	/**
	 * Maximum varchar length when creating table, defaults to 1024;
	 */
	private int maxVarcharLength = 1024;

	public MySqlDB(String dbName, String ip, String port, String username, String password) {
		this.dbName = dbName;
		this.ip = ip;
		this.port = port;

		init("com.mysql.jdbc.Driver", String.format("jdbc:mysql://%s:%s/%s", ip, port, dbName), username, password);

		this.params
			.set(MySqlDBParams.DB_NAME, dbName)
			.set(MySqlDBParams.IP, ip)
			.set(MySqlDBParams.PORT, port)
			.set(MySqlDBParams.USERNAME, username)
			.set(MySqlDBParams.PASSWORD, password);

		this.dbName = dbName;
	}

	public MySqlDB(Params params) {
		this(params.get(MySqlDBParams.DB_NAME),
			params.get(MySqlDBParams.IP),
			params.get(MySqlDBParams.PORT),
			params.get(MySqlDBParams.USERNAME),
			params.get(MySqlDBParams.PASSWORD));
	}

	/**
	 * Set the maximum varchar length in table.
	 *
	 * @param maxVarcharLength length.
	 */
	public void setMaxVarcharLength(int maxVarcharLength) {
		this.maxVarcharLength = maxVarcharLength;
	}

	@Override
	protected int getMaxVarcharLength() {
		return maxVarcharLength;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	@Override
	public Table getStreamTable(String tableName, Params params, Long sessionId) throws Exception {
		if (!params.contains(MySqlSourceParams.SCHEMA_STR)) {
			return super.getStreamTable(tableName, params, sessionId);
		} else {
			TableSchema schema = CsvUtil.schemaStr2Schema(params.get(MySqlSourceParams.SCHEMA_STR));

			JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setUsername(getUserName())
				.setPassword(getPassword())
				.setDrivername(getDriverName())
				.setDBUrl(getDbUrl())
				.setQuery("select * from " + tableName)
				.setRowTypeInfo(new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames()))
				.finish();

			return DataStreamConversionUtil.toTable(
				sessionId,
				MLEnvironmentFactory.get(sessionId).getStreamExecutionEnvironment().createInput(inputFormat),
				schema.getFieldNames(), schema.getFieldTypes());
		}
	}

	@Override
	public Table getBatchTable(String tableName, Params params, Long sessionId) throws Exception {
		if (!params.contains(MySqlSourceParams.SCHEMA_STR)) {
			return super.getBatchTable(tableName, params, sessionId);
		} else {
			TableSchema schema = CsvUtil.schemaStr2Schema(params.get(MySqlSourceParams.SCHEMA_STR));

			JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setUsername(getUserName())
				.setPassword(getPassword())
				.setDrivername(getDriverName())
				.setDBUrl(getDbUrl())
				.setQuery("select * from " + tableName)
				.setRowTypeInfo(new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames()))
				.finish();

			return DataSetConversionUtil.toTable(sessionId,
				MLEnvironmentFactory.get(sessionId).getExecutionEnvironment().createInput(inputFormat),
				schema.getFieldNames(), schema.getFieldTypes());
		}
	}
}
