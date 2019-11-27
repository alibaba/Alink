package com.alibaba.alink.common.io;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.table.BaseDbTable;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.io.directreader.DbDataBridge;
import com.alibaba.alink.params.io.HasIoName;
import com.alibaba.alink.params.io.HasSchemaStr;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;

import java.io.Serializable;
import java.util.List;

/**
 * BaseDB is the base class of the concept which is similar with database.
 *
 * <p>Most of the meta system, such as mysql, hive etc, can be abstracted like a database.
 *
 * <p>- widely used operations in database
 *
 * <p>- execute sql-like query language
 *
 * <p>- direct read data from db for loading model, which is used in prediction processing.
 *
 * <p>example:
 *
 * <p><pre>{@code
 *     BaseDB db = BaseDB.of(new Params().set("ioName", "jdbc"))
 *     Table table = db.getBatchTable("tableName", new Params())
 * }
 * </pre>
 */
public abstract class BaseDB implements Serializable {
    
    protected Params params;

    protected BaseDB(Params params) {
        if (null == params) {
            this.params = new Params();
        } else {
            this.params = params.clone();
        }
        this.params.set(HasIoName.IO_NAME, AnnotationUtils.annotatedName(this.getClass()));
    }

    public static BaseDB of(Params params) throws Exception {
        if (BaseDB.isDB(params)) {
            return AnnotationUtils.createDB(params.get(HasIoName.IO_NAME), params);
        } else {
            throw new RuntimeException("NOT a DB parameter.");
        }
    }

    public static boolean isDB(Params params) {
        String name = params.get(HasIoName.IO_NAME);

		if (params.contains(HasIoName.IO_NAME)) {
			return AnnotationUtils.isDB(params.get(HasIoName.IO_NAME));
		} else {
			return false;
		}
	}

    /**
     * Get name of db.
     *
     * @return
     */
    public abstract String getName();

    /**
     * List table names in DB.
     *
     * @return table name list.
     * @throws Exception
     */
    public abstract List<String> listTableNames() throws Exception;

    /**
     * Execute an SQL statement on DB instance.
     *
     * @param sql the SQL statement to execute.
     * @throws Exception
     */
    public abstract void execute(String sql) throws Exception;

    /**
     * Create a table.
     *
     * @param tableName table name.
     * @param schema    table schema.
     * @return
     * @throws Exception
     */
    public void createTable(String tableName, TableSchema schema) throws Exception {
        createTable(tableName, schema, null);
    }

    /**
     * Create a table with table schema retrieved from parameter.
     *
     * @param tableName table name.
     * @param parameter parameter must has a key named `schemaStr`. It's value should follow the format: <br>
     *                  <ul>
     *                  <li>[colName colType](,[colName colType])+<br></li>
     *                  </ul>
     *                  Valid `colType` can be, "string", "double", "long", "boolean" and "timestamp".
     *                  For example:
     *                  <ul>
     *                  <li>"id int"</li>
     *                  <li>"id int, name string, weight double"</li>
     *                  </ul>
     * @throws Exception
     */
    public void createTable(String tableName, Params parameter) throws Exception {
        createTable(tableName, retrieveTableSchema(parameter), parameter);
    }

    /**
     * Create a table.
     *
     * @param tableName table name.
     * @param schema    table schema.
     * @param parameter other parameters.
     * @throws Exception
     */
    public abstract void createTable(String tableName, TableSchema schema, Params parameter) throws Exception;

    /**
     * Drop table.
     *
     * @param tableName table name.
     * @throws Exception if something wrong.
     */
    public abstract void dropTable(String tableName) throws Exception;

    /**
     * Check the existence of a table.
     *
     * @param table table name.
     * @return true if the table exists.
     * @throws Exception
     */
    public abstract boolean hasTable(String table) throws Exception;

    /**
     * Check the existence of a column.
     *
     * @param table  table name.
     * @param column column name.
     * @return true if the column exists.
     * @throws Exception
     */
    public abstract boolean hasColumn(String table, String column) throws Exception;

    /**
     * Get columns of a table.
     *
     * @param tableName table name.
     * @return Columns names in an array.
     * @throws Exception
     */
    public abstract String[] getColNames(String tableName) throws Exception;

    /**
     * Get table schema of table.
     *
     * @param tableName tabla name.
     * @return
     * @throws Exception
     */
    public abstract TableSchema getTableSchema(String tableName) throws Exception;

    /**
     * Helper function which retrieve table schema from parameter.
     *
     * @param parameter parameter must has a key named `schemaStr`. It's value should follow the format: <br>
     *                  <ul>
     *                  <li>[colName colType](,[colName colType])+<br></li>
     *                  </ul>
     *                  Valid `colType` can be, "string", "double", "long", "boolean" and "timestamp".
     *                  For example:
     *                  <ul>
     *                  <li>"id int"</li>
     *                  <li>"id int, name string, weight double"</li>
     *                  </ul>
     * @return retrieved table schema.
     */
    private static TableSchema retrieveTableSchema(Params parameter) {
        String tableSchmaStr = parameter.get(HasSchemaStr.SCHEMA_STR);
        if (tableSchmaStr == null || tableSchmaStr.length() == 0) {
            throw new RuntimeException("table schema is empty.");
        }

        String[] kvs = tableSchmaStr.split(",");
        String[] colNames = new String[kvs.length];
        TypeInformation<?>[] colTypes = new TypeInformation<?>[kvs.length];

        for (int i = 0; i < kvs.length; i++) {
            String[] splits = kvs[i].split(" ");
            if (splits.length != 2) {
                throw new RuntimeException("table schema error. " + tableSchmaStr);
            }
            colNames[i] = splits[0];
            switch (splits[1].trim().toLowerCase()) {
                case "string":
                    colTypes[i] = Types.STRING;
                    break;
                case "double":
                    colTypes[i] = Types.DOUBLE;
                    break;
                case "long":
                    colTypes[i] = Types.LONG;
                    break;
                case "boolean":
                    colTypes[i] = Types.BOOLEAN;
                    break;
                case "timestamp":
                    colTypes[i] = Types.SQL_TIMESTAMP;
                    break;
                default:
                    break;
            }
        }

        return new TableSchema(colNames, colTypes);
    }

    /**
     * Close this database instance.
     *
     * @throws Exception
     */
    public abstract void close() throws Exception;

    /**
     * Get a {@link BaseDbTable} object representing an table.
     *
     * @param tableName table name
     * @return {@link BaseDbTable} object.
     * @throws Exception
     */
    public abstract BaseDbTable getDbTable(String tableName) throws Exception;

    /**
     * Get Flink Table object of given table in default ML environment.
     *
     * @param tableName table name
     * @param parameter parameter
     * @return flink table.
     * @throws Exception
     */
    public Table getStreamTable(String tableName, Params parameter) throws Exception {
        return getStreamTable(tableName, parameter,
            parameter == null ? MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID :
                parameter.get(HasMLEnvironmentId.ML_ENVIRONMENT_ID));
    }

    /**
     * Get Flink Table object of given table.
     *
     * @param tableName table name
     * @param parameter parameter.
     * @param sessionId ML Environment session id.
     * @return flink table.
     * @throws Exception
     */
    public abstract Table getStreamTable(String tableName, Params parameter, Long sessionId) throws Exception;

    /**
     * Sink data in flink table into target table in database with default ML Environment.
     *
     * @param tableName target table in database.
     * @param in        flink table to sink.
     * @param parameter params.
     */
    public void sinkStream(String tableName, Table in, Params parameter) {
        sinkStream(tableName, in, parameter,
            parameter == null ? MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID :
                parameter.get(HasMLEnvironmentId.ML_ENVIRONMENT_ID));
    }

    /**
     * Sink a data in flink table into target table in database.
     *
     * @param tableName target table in database.
     * @param in        flink table to sink.
     * @param sessionId session id.
     * @param parameter params.
     */
    public abstract void sinkStream(String tableName, Table in, Params parameter, Long sessionId);

    public void bucketingSinkStream(String tableName, Table in, Params parameter, TableSchema schema) {
        bucketingSinkStream(tableName, in, parameter, schema,
            parameter == null ? MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID :
                parameter.get(HasMLEnvironmentId.ML_ENVIRONMENT_ID));
    }

    public void bucketingSinkStream(String tableName, Table in, Params parameter, TableSchema schema,
                                    Long sessionId) {
        TableBucketingSink oof = new TableBucketingSink(tableName, parameter, schema, this);

        DataStreamConversionUtil.fromTable(sessionId, in).addSink(oof);

    }

    /**
     * Get Flink Table object of given table in default ML environment.
     *
     * @param tableName table name
     * @param parameter parameter
     * @return flink table.
     * @throws Exception
     */
    public Table getBatchTable(String tableName, Params parameter) throws Exception {
        return getBatchTable(tableName, parameter,
            parameter == null ? MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID :
                parameter.get(HasMLEnvironmentId.ML_ENVIRONMENT_ID));
    }

    /**
     * Get Flink Table object of given table.
     *
     * @param tableName table name
     * @param parameter parameter.
     * @param sessionId ML Environment session id.
     * @return flink table.
     * @throws Exception
     */
    public abstract Table getBatchTable(String tableName, Params parameter, Long sessionId) throws Exception;

    /**
     * Sink a data in flink table into target table in database.
     *
     * @param tableName target table in database.
     * @param in        flink table to sink.
     * @param sessionId session id.
     * @param parameter params.
     */
    public abstract void sinkBatch(String tableName, Table in, Params parameter, Long sessionId);

    /**
     * Create Flink OutputFormat from a table in db.
     *
     * @param tableName table name
     * @param schema    format schema.
     * @return Flink OutputFormat
     */
    public abstract RichOutputFormat createFormat(String tableName, TableSchema schema);

    /**
     * Create a DbDataBridge.
     *
     * @param connector
     * @return
     * @throws Exception
     */
    public DbDataBridge initConnector(DbDataBridge connector) throws Exception {
        throw new Exception("Unsupported direct read now.");
    }

    public List<Row> directRead(DbDataBridge connector, FilterFunction<Row> filter) throws Exception {
        throw new Exception("Unsupported direct read now.");
    }

    public Params getParams() {
        return this.params;
    }

}

