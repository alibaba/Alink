package com.alibaba.alink.common.io;

import org.apache.flink.ml.api.misc.param.Params;

import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.io.annotations.DBAnnotation;
import com.alibaba.alink.params.io.DerbyDBParams;

import java.sql.DriverManager;
import java.sql.SQLException;


/**
 * The Derby DB.
 */
@DBAnnotation(name = "derby")
public class DerbyDB extends JdbcDB {

    private String dbName;

    public DerbyDB(String dbName) throws SQLException {
        this(dbName, null, null);

        this.dbName = dbName;
    }

    public DerbyDB(String dbName, String username, String password)
            throws SQLException {
        this.params
                .set(DerbyDBParams.DB_NAME, dbName)
                .set(DerbyDBParams.USERNAME, username)
                .set(DerbyDBParams.PASSWORD, password);
        this.dbName = dbName;
        init("org.apache.derby.jdbc.EmbeddedDriver", String.format("jdbc:derby:%s", dbName), username, password);
        DriverManager.getConnection(String.format("jdbc:derby:%s;create=true", dbName), username, password);
    }

    public DerbyDB(Params params) throws SQLException {
        this(params.get(DerbyDBParams.DB_NAME),
                params.get(DerbyDBParams.USERNAME),
                params.get(DerbyDBParams.PASSWORD));
    }

    @Override
    public boolean hasColumn(String tableName, String columnName)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
        return super.hasColumn(tableName.toUpperCase(), columnName.toUpperCase());
    }

    @Override
    public boolean hasTable(String table) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        return super.hasTable(table.toUpperCase());
    }

    @Override
    public TableSchema getTableSchema(String tableName)
            throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        return super.getTableSchema(tableName.toUpperCase());
    }

    public String getDbName() {
        return dbName;
    }
}
