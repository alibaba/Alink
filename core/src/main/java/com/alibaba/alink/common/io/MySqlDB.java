package com.alibaba.alink.common.io;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.annotations.DBAnnotation;
import com.alibaba.alink.params.io.MySqlDBParams;

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

}
