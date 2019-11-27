package com.alibaba.alink.common.io.table;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.JdbcDB;

public class JdbcTable extends BaseDbTable {

	private JdbcDB jdbcDB;
	private String tableName;

	public JdbcTable(JdbcDB jdbcDB, String tableName) {
		this.jdbcDB = jdbcDB;
		this.tableName = tableName;

	}

	@Override
	public String getOwner() {
		return this.jdbcDB.getUserName();
	}

	@Override
	public String getTableName() {
		return this.tableName;
	}

	@Override
	public String[] getColNames() {
		try {
			return this.jdbcDB.getTableSchema(this.tableName).getFieldNames();
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	@Override
	public Class[] getColTypes() {
		try {
			TypeInformation <?>[] types = this.jdbcDB.getTableSchema(this.tableName).getFieldTypes();
			Class[] comments = new Class[types.length];
			int i = 0;
			for (TypeInformation type : types) {
				if (type == BasicTypeInfo.DOUBLE_TYPE_INFO) {
					comments[i] = Double.class;
				} else if (type == BasicTypeInfo.INT_TYPE_INFO ||
					type == BasicTypeInfo.LONG_TYPE_INFO) {
					comments[i] = Long.class;
				} else if (type == BasicTypeInfo.STRING_TYPE_INFO) {
					comments[i] = String.class;
				} else {

				}
				i++;
			}
			return comments;
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	@Override
	public TableSchema getSchema() {
		try {
			return this.jdbcDB.getTableSchema(this.tableName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public BaseDB getDB() {
		return this.jdbcDB;
	}

}
