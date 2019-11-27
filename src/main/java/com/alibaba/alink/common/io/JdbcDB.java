package com.alibaba.alink.common.io;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.DBAnnotation;
import com.alibaba.alink.common.io.table.BaseDbTable;
import com.alibaba.alink.common.io.table.JdbcTable;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.types.JdbcTypeConverter;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.stream.sink.JdbcRetractSinkStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.params.io.JdbcDBParams;
import com.alibaba.alink.params.shared.HasOverwriteSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * The implement of BaseDB using jdbc.
 */
@DBAnnotation(name = "jdbc")
public class JdbcDB extends BaseDB {

    private final static Logger LOG = LoggerFactory.getLogger(JdbcDB.class);

    public static int MAX_VARCHAR_SIZE = 30000;

    private String userName;
    private String password;
    private String driverName;
    private String dbUrl;
    private transient Connection conn;

    public JdbcDB() {
        super(null);
    }

    public JdbcDB(String driverName, String dbUrl) {
        this(driverName, dbUrl, null, null);
    }

    public JdbcDB(String driverName, String dbUrl, String userName, String password) {
        super(new Params()
                .set(JdbcDBParams.DRIVER_NAME, driverName)
                .set(JdbcDBParams.URL, dbUrl)
                .set(JdbcDBParams.USERNAME, userName)
                .set(JdbcDBParams.PASSWORD, password)
        );
        init(driverName, dbUrl, userName, password);
    }

    public JdbcDB(Params params) {
        super(params);
        init(
                params.get(JdbcDBParams.DRIVER_NAME),
                params.get(JdbcDBParams.URL),
                params.get(JdbcDBParams.USERNAME),
                params.get(JdbcDBParams.PASSWORD)
        );
    }

    protected void init(String driverName, String dbUrl, String username, String password) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.driverName = driverName;
        this.dbUrl = dbUrl;
        this.userName = username;
        this.password = password;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getDriverName() {
        return driverName;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    protected Connection getConn()
            throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {
        if (null == this.conn) {
            Class.forName(this.driverName);
            if (null == userName) {
                this.conn = DriverManager.getConnection(this.dbUrl);
            } else {
                this.conn = DriverManager.getConnection(this.dbUrl, this.userName, this.password);
            }
        }
        return this.conn;
    }

    protected int getMaxVarcharLength() {
        return MAX_VARCHAR_SIZE;
    }


    @Override
    public List<String> listTableNames()
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        DatabaseMetaData meta = getConn().getMetaData();
        ResultSet rs = meta.getTables(null, null, null, new String[]{"TABLE"});
        ArrayList<String> tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"));
        }
        return tables;
    }

    /**
     * Execute an SQL statement with {@link Statement#executeUpdate(String)}.
     *
     * @param sql the SQL statement to execute.
     * @return Row count for DML statements or 0 for SQL statements that return nothing.
     * @throws ClassNotFoundException same as {@link Statement#executeUpdate(String)}.
     * @throws SQLException           same as {@link Statement#executeUpdate(String)}.
     * @throws InstantiationException same as {@link Statement#executeUpdate(String)}.
     * @throws IllegalAccessException same as {@link Statement#executeUpdate(String)}.
     */
    public int executeUpdate(String sql)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        try (Statement stmt = getConn().createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }

    /**
     * Execute an SQL statement with {@link Statement#execute(String)}.
     *
     * @param sql the SQL statement to execute.
     * @throws ClassNotFoundException same as {@link Statement#execute(String)}.
     * @throws SQLException           same as {@link Statement#execute(String)}.
     * @throws InstantiationException same as {@link Statement#execute(String)}.
     * @throws IllegalAccessException same as {@link Statement#execute(String)}.
     */
    @Override
    public void execute(String sql)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        try (Statement stmt = getConn().createStatement()) {
            stmt.execute(sql);
        }
    }

    @FunctionalInterface
    public interface SQLConsumer<T> {
        void accept(T t) throws SQLException;
    }

    @FunctionalInterface
    public interface SQLFunction<T, U> {
        U apply(T t) throws SQLException;
    }


    /**
     * Execute a SQL statement and consume the ResultSet.
     *
     * @param query    SQL statement
     * @param consumer An consumer which receives as input a ResultSet and return nothing.
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public void query(String query, SQLConsumer<ResultSet> consumer)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        try (Statement stmt = getConn().createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            consumer.accept(rs);
        }
    }

    /**
     * Execute a SQL statement and apply the ResultSet the a function and return the function's result.
     *
     * @param query SQL statement
     * @param func  An function which receives as input a ResultSet and return some value.
     * @param <T>   Result type of function.
     * @return the function's return value.
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public <T> T query(String query, SQLFunction<ResultSet, T> func)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        try (Statement stmt = getConn().createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            return func.apply(rs);
        }
    }

    @Override
    public void createTable(String tableName, TableSchema schema, Params parameter)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        StringBuilder sbd = new StringBuilder();
        sbd.append("create table ").append(tableName).append(" (");
        String[] colNames = schema.getFieldNames();
        TypeInformation<?>[] colTypes = schema.getFieldTypes();
        int n = colNames.length;
        for (int i = 0; i < n; i++) {
            String type = FlinkTypeConverter.getTypeString(colTypes[i]);
            sbd.append(colNames[i]).append(" ").append(type);
            if ("VARCHAR".equalsIgnoreCase(type)) {
                sbd.append("(").append(getMaxVarcharLength()).append(")");
            }
            if (i < n - 1) {
                sbd.append(",");
            }
        }
        sbd.append(")");
        String sql = sbd.toString();
        LOG.info("JdbcDB create table {}: {}", tableName, sql);
        execute(sql);
    }

    public boolean createTable(String tableName, TableSchema schema, String[] primaryKeys)
            throws Exception {
        throw new UnsupportedOperationException("createTable with primary keys");
    }

    @Override
    public void dropTable(String tableName)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        execute("DROP TABLE " + tableName);
    }

    @Override
    public boolean hasTable(String table)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        try (ResultSet rs = getConn().getMetaData().getTables(null, null, table, null)) {
            return rs.next();
        }
    }

    @Override
    public boolean hasColumn(String tableName, String columnName)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
        try (ResultSet rs = getConn().getMetaData().getColumns(null, null, tableName, columnName)) {
            return rs.next();
        }
    }

    @Override
    public String[] getColNames(String tableName)
            throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        TableSchema tableSchema = getTableSchema(tableName);
        return tableSchema.getFieldNames();
    }

    @Override
    public TableSchema getTableSchema(String tableName)
            throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        ResultSet rs = getConn().getMetaData().getColumns(null, null, tableName, null);
        List<String> colNames = new ArrayList<>();
        List<TypeInformation<?>> colTypes = new ArrayList<>();
        while (rs.next()) {
            if (tableName.equals(rs.getString("TABLE_NAME"))) {
                String colName = rs.getString("COLUMN_NAME");
                int dataType = rs.getInt("DATA_TYPE");
                colNames.add(colName);
                colTypes.add(JdbcTypeConverter.getFlinkType(dataType));
            }
        }
        return new TableSchema(colNames.toArray(new String[0]), colTypes.toArray(new TypeInformation<?>[0]));
    }

    public String[] getPrimaryKeys(String tableName) throws Exception {
        throw new UnsupportedOperationException("getPrimaryKeys");
    }

    @Override
    public void close() throws SQLException {
        if (null != this.conn) {
            this.conn.close();
            this.conn = null;
        }
    }

    @Override
    protected void finalize() {
        try {
            close();
        } catch (SQLException e) {
            LOG.error("exception when close JdbcDB in finalize()", e);
        }
    }

    @Override
    public BaseDbTable getDbTable(String tableName) throws Exception {
        return new JdbcTable(this, tableName);
    }

    @Override
    public String getName() {
        return this.dbUrl.split(":", 3)[2];
    }

    @Override
    public Table getStreamTable(String tableName, Params parameter, Long sessionId) throws Exception {
        TableSchema schema = getTableSchema(tableName);
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

    @Override
    public void sinkStream(String tableName, Table in, Params parameter, Long sessionId) {
        try {
            if (!this.hasTable(tableName)) {
                this.createTable(tableName, in.getSchema(), parameter);
            }
        } catch (Exception e) {
            throw new RuntimeException("Fail to create table: " + e);
        }

        TableSchema schema = in.getSchema();
        String[] colNames = schema.getFieldNames();
        StringBuilder sbd = new StringBuilder();
        sbd.append("INSERT INTO ").append(tableName).append(" (").append(colNames[0]);
        for (int i = 1; i < colNames.length; i++) {
            sbd.append(",").append(colNames[i]);
        }
        sbd.append(") VALUES (?");
        for (int i = 1; i < colNames.length; i++) {
            sbd.append(",").append("?");
        }
        sbd.append(")");

        String sql = sbd.toString();
        LOG.info("JdbcDB sink stream to table {}: {}", tableName, sql);

        String[] primaryColNames = parameter.getStringArrayOrDefault("primaryKeys", null);

        if (primaryColNames == null || primaryColNames.length == 0) {
            JDBCAppendTableSink jdbcAppendTableSink = JDBCAppendTableSink.builder()
                    .setUsername(getUserName())
                    .setPassword(getPassword())
                    .setDrivername(getDriverName())
                    .setDBUrl(getDbUrl())
                    .setQuery(sql)
                    .setParameterTypes(schema.getFieldTypes())
                    .build();
            StreamTableEnvironment tEnv = MLEnvironmentFactory.get(sessionId).getStreamTableEnvironment();
            jdbcAppendTableSink.emitDataStream(tEnv
                    .toAppendStream(in, new RowTypeInfo(in.getSchema().getFieldTypes()))
            );
        } else {
            new TableSourceStreamOp(in)
                    .setMLEnvironmentId(sessionId)
                    .link(new JdbcRetractSinkStreamOp(this, tableName, primaryColNames));
        }
    }

    @Override
    public Table getBatchTable(String tableName, Params parameter, Long sessionId) throws Exception {
        TableSchema schema = getTableSchema(tableName);
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

    private static void dropAndCreateTable(BaseDB db, String tableName, Table in, Params params) {

        boolean isOverwriteSink = params.get(HasOverwriteSink.OVERWRITE_SINK);

        //Create Table
        try {
            if (isOverwriteSink) {
                if (db.hasTable(tableName)) {
                    db.dropTable(tableName);
                }
            }
            db.createTable(tableName, in.getSchema(), params);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sinkBatch(String tableName, Table in, Params parameter, Long sessionId) {
        dropAndCreateTable(this, tableName, in, parameter);

        TableSchema schema = in.getSchema();
        String[] colNames = schema.getFieldNames();
        StringBuilder sbd = new StringBuilder();
        sbd.append("INSERT INTO ").append(tableName).append(" (").append(colNames[0]);
        for (int i = 1; i < colNames.length; i++) {
            sbd.append(",").append(colNames[i]);
        }
        sbd.append(") VALUES (?");
        for (int i = 1; i < colNames.length; i++) {
            sbd.append(",").append("?");
        }
        sbd.append(")");

        JDBCAppendTableSink jdbcAppendTableSink = JDBCAppendTableSink.builder()
                .setUsername(getUserName())
                .setPassword(getPassword())
                .setDrivername(getDriverName())
                .setDBUrl(getDbUrl())
                .setQuery(sbd.toString())
                .setParameterTypes(schema.getFieldTypes())
                .build();

        jdbcAppendTableSink.emitDataSet(BatchOperator.fromTable(in).setMLEnvironmentId(sessionId).getDataSet());
    }

    @Override
    public RichOutputFormat createFormat(String tableName, TableSchema schema) {
        TypeInformation[] types = schema.getFieldTypes();
        String[] colNames = schema.getFieldNames();
        int[] parameterTypes = new int[types.length];
        for (int i = 0; i < types.length; ++i) {
            parameterTypes[i] = JdbcTypeConverter.getIntegerSqlType(types[i]);
        }
        StringBuilder sbd = new StringBuilder();
        sbd.append("INSERT INTO ").append(tableName).append(" (").append(colNames[0]);
        for (int i = 1; i < colNames.length; i++) {
            sbd.append(",").append(colNames[i]);
        }
        sbd.append(") VALUES (?");
        for (int i = 1; i < colNames.length; i++) {
            sbd.append(",").append("?");
        }
        sbd.append(")");

        return JDBCOutputFormat.buildJDBCOutputFormat()
                .setUsername(userName)
                .setPassword(password)
                .setDBUrl(getDbUrl())
                .setQuery(sbd.toString())
                .setDrivername(getDriverName())
                .setBatchInterval(5000)
                .setSqlTypes(parameterTypes)
                .finish();
    }
}


