package com.alibaba.alink.operator.common.io.jdbc;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.TableUtil;
import org.apache.flink.api.common.typeinfo.Types;
import com.alibaba.alink.common.io.JdbcDB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Output format for JDBC with upser(update and insert) semantic.
 */
public class JDBCUpserOutputFormat extends RichOutputFormat <Tuple2 <Boolean, Row>> {
	private static final long serialVersionUID = 1L;
	private String username;
	private String password;
	private String drivername;
	private String dbURL;

	private JdbcDB jdbcDB = null;

	private String tableName;
	private String[] primaryColNames;

	private TypeInformation[] primaryColTypes;
	private String[] otherColNames; //colnames without primary col
	private TypeInformation[] otherColTypes;
	private String[] colNames;
	private TypeInformation[] colTypes;
	private int N;

	public JDBCUpserOutputFormat(String username, String password, String drivername, String dbURL, String tableName,
								 String[] primaryColNames) {
		this.username = username;
		this.password = password;
		this.drivername = drivername;
		this.dbURL = dbURL;
		this.tableName = tableName;
		this.primaryColNames = primaryColNames;
	}

	@Override
	public void configure(Configuration configuration) {

	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			if (this.username == null || this.username.isEmpty()) {
				jdbcDB = new JdbcDB(drivername, dbURL);
			} else {
				jdbcDB = new JdbcDB(drivername, dbURL, username, password);
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}

		try {
			TableSchema schema = this.jdbcDB.getTableSchema(tableName);
			colNames = schema.getFieldNames();
			colTypes = schema.getFieldTypes();
			N = colNames.length;
			if (primaryColNames != null && primaryColNames.length != 0) {
				for (int i = 0; i < primaryColNames.length; i++) {
					if (TableUtil.findColIndex(colNames, primaryColNames[i]) < 0) {
						throw new RuntimeException("primary col " + primaryColNames[i] + " not exsit.");
					}
				}
				List <String> otherCols = new ArrayList <>();
				for (int i = 0; i < colNames.length; i++) {
					if (TableUtil.findColIndex(primaryColNames, colNames[i]) < 0) {
						otherCols.add(colNames[i]);
					}
				}
				this.otherColNames = otherCols.toArray(new String[0]);
				if (this.otherColNames.length == 0) {
					throw new RuntimeException("primary col names must be not all table colnames.");
				}

				this.primaryColTypes = new TypeInformation[this.primaryColNames.length];
				for (int i = 0; i < this.primaryColNames.length; i++) {
					this.primaryColTypes[i] = this.colTypes[TableUtil.findColIndex(colNames,
						this.primaryColNames[i])];
				}
				this.otherColTypes = new TypeInformation[this.otherColNames.length];
				for (int i = 0; i < this.otherColNames.length; i++) {
					this.otherColTypes[i] = this.colTypes[TableUtil.findColIndex(colNames, otherColNames[i])];
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	@Override
	public void writeRecord(Tuple2 <Boolean, Row> tuple2) throws IOException {
		if (!tuple2.f0) {
			return;
		}

		Row row = tuple2.f1;

		try {
			if (primaryColNames == null || primaryColNames.length == 0) {
				jdbcDB.execute(getInsertSql(row));
			} else {
				String sql = getUpdataSql(row);
				int count = jdbcDB.executeUpdate(sql);
				if (count == 0) {
					sql = getInsertSql(row);
					jdbcDB.execute(sql);
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	private String convert2String(Object val, TypeInformation type) {
		if (type == Types.STRING || type == Types.SQL_DATE || type == Types.SQL_TIME) {
			return "'" + val + "'";
		} else {
			return String.valueOf(val);
		}
	}

	//"insert into ning_test values('test', 'aa')"
	private String getInsertSql(Row row) {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ")
			.append(tableName)
			.append(" values(")
			.append(convert2String(row.getField(0), colTypes[0]));
		for (int i = 1; i < N; i++) {
			sb.append(", ");
			sb.append(convert2String(row.getField(i), colTypes[i]));
		}
		sb.append(")");
		return sb.toString();
	}

	//"update  ning_test set f1='cc' where f0='testx'"
	private String getUpdataSql(Row row) {
		StringBuilder sb = new StringBuilder();
		int idx = TableUtil.findColIndex(colNames, this.otherColNames[0]);
		sb.append("update ").append(this.tableName).append(" set ")
			.append(otherColNames[0])
			.append("=")
			.append(convert2String(row.getField(idx), colTypes[idx]));
		for (int i = 1; i < otherColNames.length; i++) {
			idx = TableUtil.findColIndex(colNames, this.otherColNames[i]);
			sb.append(", ")
				.append(otherColNames[i]).
				append("=").
				append(convert2String(row.getField(idx), colTypes[idx]));
		}

		idx = TableUtil.findColIndex(colNames, this.primaryColNames[0]);
		sb.append(" where ")
			.append(primaryColNames[0]).append("=").append(convert2String(row.getField(idx), colTypes[idx]));

		for (int i = 1; i < primaryColNames.length; i++) {
			idx = TableUtil.findColIndex(colNames, this.primaryColNames[i]);
			sb.append(" and  ").
				append(primaryColNames[i]).
				append("=").
				append(convert2String(row.getField(idx), colTypes[idx]));
		}

		return sb.toString();
	}

	@Override
	public void close() throws IOException {
		try {
			jdbcDB.close();
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}
}
