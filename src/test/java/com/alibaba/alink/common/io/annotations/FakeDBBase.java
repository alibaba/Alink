package com.alibaba.alink.common.io.annotations;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.table.BaseDbTable;

import java.util.List;

/**
 * An simple helper class to simplify creation of FakeDB.
 *
 */
public class FakeDBBase extends BaseDB {

    protected FakeDBBase(Params params) {
        super(params);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public List<String> listTableNames() throws Exception {
        return null;
    }

    @Override
    public void execute(String sql) throws Exception {

    }

    @Override
    public void createTable(String tableName, TableSchema schema, Params parameter) throws Exception {

    }

    @Override
    public void dropTable(String tableName) throws Exception {

    }

    @Override
    public boolean hasTable(String table) throws Exception {
        return false;
    }

    @Override
    public boolean hasColumn(String table, String column) throws Exception {
        return false;
    }

    @Override
    public String[] getColNames(String tableName) throws Exception {
        return new String[0];
    }

    @Override
    public TableSchema getTableSchema(String tableName) throws Exception {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public BaseDbTable getDbTable(String tableName) throws Exception {
        return null;
    }

    @Override
    public Table getStreamTable(String tableName, Params parameter, Long sessionId) throws Exception {
        return null;
    }

    @Override
    public void sinkStream(String tableName, Table in, Params parameter, Long sessionId) {

    }

    @Override
    public Table getBatchTable(String tableName, Params parameter, Long sessionId) throws Exception {
        return null;
    }

    @Override
    public void sinkBatch(String tableName, Table in, Params parameter, Long sessionId) {

    }

    @Override
    public RichOutputFormat createFormat(String tableName, TableSchema schema) {
        return null;
    }
}
