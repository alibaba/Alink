package com.alibaba.alink.common.io.catalog.mysql;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialects;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.catalog.JdbcCatalog;
import com.alibaba.alink.params.io.JdbcCatalogParams;
import com.alibaba.alink.params.io.MySqlCatalogParams;

import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MySqlCatalog extends JdbcCatalog {

	private static final JDBCDialect MYSQL_DIALECT = JDBCDialects
		.get("jdbc:mysql:")
		.orElseThrow(
			() -> new IllegalArgumentException("Could not find the mysql dialect.")
		);

	public static final String DEFAULT_DATABASE = "default";

	public MySqlCatalog(
		String catalogName, String defaultDatabase, String mysqlUrl, String port,
		String userName, String password) {

		this(
			new Params()
				.set(MySqlCatalogParams.DRIVER_NAME, MYSQL_DIALECT.defaultDriverName().orElse("com.mysql.jdbc.Driver"))
				.set(MySqlCatalogParams.CATALOG_NAME, catalogName == null ? genRandomCatalogName() : catalogName)
				.set(MySqlCatalogParams.DEFAULT_DATABASE, defaultDatabase == null ? DEFAULT_DATABASE : defaultDatabase)
				.set(MySqlCatalogParams.URL, genMySqlJdbcUrl(mysqlUrl, port, defaultDatabase))
				.set(MySqlCatalogParams.USERNAME, userName)
				.set(MySqlCatalogParams.PASSWORD, password)
		);
	}

	@Override
	public void open() throws CatalogException {
		try {
			Class.forName(getParams().get(JdbcCatalogParams.DRIVER_NAME));

			if (getParams().contains(JdbcCatalogParams.USERNAME) && !getParams().contains(JdbcCatalogParams.PASSWORD)) {
				connection = DriverManager.getConnection(getParams().get(JdbcCatalogParams.URL));
			} else {
				connection = DriverManager.getConnection(
					getParams().get(JdbcCatalogParams.URL),
					getParams().get(JdbcCatalogParams.USERNAME),
					getParams().get(JdbcCatalogParams.PASSWORD)
				);
			}

		} catch (ClassNotFoundException | SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	private static String genMySqlJdbcUrl(String dbUrl, String port, String database) {
		StringBuilder stringBuilder = new StringBuilder();

		stringBuilder.append("jdbc:mysql://");
		stringBuilder.append(dbUrl);
		if (port != null) {
			stringBuilder.append(":").append(port);
		}
		if (database != null) {
			stringBuilder.append("/").append(database);
		}

		return stringBuilder.toString();
	}

	public MySqlCatalog(Params params) {
		super(params);
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName)
		throws DatabaseNotExistException, CatalogException {

		if (databaseExists(databaseName)) {
			return new CatalogDatabaseImpl(Collections.emptyMap(), null);
		} else {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
	}

	@Override
	public List <String> listDatabases() throws CatalogException {
		try {
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet rs = meta.getCatalogs();
			ArrayList <String> catalogs = new ArrayList <>();
			while (rs.next()) {
				catalogs.add(rs.getString(1));
			}
			return catalogs;
		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException, CatalogException {

		try {
			boolean databaseExists = databaseExists(name);

			if (ignoreIfExists && databaseExists) {
				return;
			} else if (!ignoreIfExists && databaseExists) {
				throw new DatabaseAlreadyExistException(getName(), name);
			}

			executeSql(String.format("CREATE DATABASE %s", name));
		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		return listDatabases().contains(databaseName);
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
		try {
			executeSql(String.format("DROP DATABASE %s", name));
		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public List <String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {

		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}

		try {
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet rs = meta.getTables(databaseName, null, null, new String[] {"TABLE"});
			ArrayList <String> tables = new ArrayList <>();
			while (rs.next()) {
				tables.add(rs.getString("TABLE_NAME"));
			}

			rs = meta.getTables(databaseName, null, null, new String[] {"VIEW"});
			while (rs.next()) {
				tables.add(rs.getString("TABLE_NAME"));
			}
			return tables;
		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		List <String> tables;
		try {
			tables = listTables(tablePath.getDatabaseName());
		} catch (DatabaseNotExistException e) {
			return false;
		}

		return tables.contains(tablePath.getObjectName());
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

		boolean tableExists = tableExists(tablePath);

		if (ignoreIfExists && tableExists) {
			return;
		} else if (!ignoreIfExists && tableExists) {
			throw new TableAlreadyExistException(getName(), tablePath);
		}

		try {
			StringBuilder sbd = new StringBuilder();

			sbd.append("CREATE TABLE ")
				.append(tablePath.getFullName())
				.append(" (");

			TableSchema schema = table.getSchema();

			for (int i = 0; i < schema.getFieldCount(); i++) {

				if (i > 0) {
					sbd.append(", ");
				}

				sbd
					.append(
						schema.getFieldName(i)
							.orElseThrow(() -> new IllegalArgumentException("Counld not find the column."))
					)
					.append(" ")
					.append(flinkType2MySqlColumnDefinition(schema.getFieldDataType(i).orElseThrow(
						() -> new IllegalArgumentException("Counld not find the column."))));
			}

			sbd.append(")");

			executeSql(sbd.toString());

		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {

		if (tableExists(tablePath)) {
			try {

				PreparedStatement ps = connection.prepareStatement(
					String.format("SELECT * FROM %s;", tablePath.getFullName()));

				ResultSetMetaData rsmd = ps.getMetaData();

				String[] names = new String[rsmd.getColumnCount()];
				DataType[] types = new DataType[rsmd.getColumnCount()];

				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					names[i - 1] = rsmd.getColumnName(i);
					types[i - 1] = mysqlType2FlinkType(rsmd, i);
					if (rsmd.isNullable(i) == ResultSetMetaData.columnNoNulls) {
						types[i - 1] = types[i - 1].notNull();
					}
				}

				TableSchema.Builder tableBuilder = new TableSchema.Builder()
					.fields(names, types);

				return new CatalogTableImpl(
					tableBuilder.build(),
					Collections.emptyMap(), null
				);
			} catch (SQLException ex) {
				throw new CatalogException(ex);
			}
		} else {
			throw new TableNotExistException(getName(), tablePath);
		}
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {

		boolean tableExists = tableExists(tablePath);

		if (ignoreIfNotExists && !tableExists) {
			return;
		} else if (!ignoreIfNotExists && !tableExists) {
			throw new TableNotExistException(getName(), tablePath);
		}

		try {
			executeSql(String.format("DROP TABLE %s", tablePath.getFullName()));
		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, TableAlreadyExistException, CatalogException {

		boolean tableExists = tableExists(tablePath);

		if (ignoreIfNotExists && !tableExists) {
			return;
		} else if (!ignoreIfNotExists && !tableExists) {
			throw new TableNotExistException(getName(), tablePath);
		}

		try {
			executeSql(String.format("RENAME TABLE %s TO %s", tablePath.getFullName(),
				new ObjectPath(tablePath.getDatabaseName(), newTableName).getFullName()));
		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public List <String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		try {
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet rs = meta.getTables(databaseName, null, null, new String[] {"VIEW"});
			ArrayList <String> views = new ArrayList <>();
			while (rs.next()) {
				views.add(rs.getString("TABLE_NAME"));
			}
			return views;
		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	/**
	 * Ref: https://dev.mysql.com/doc/refman/8.0/en/data-types.html
	 *
	 * @see {@link com.mysql.jdbc.ResultSetMetaData#getColumnTypeName(int)}
	 */

	// the following items are copy from jdbc class.
	public static final String MYSQL_BIT = "BIT";
	public static final String MYSQL_DECIMAL = "DECIMAL";
	public static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
	public static final String MYSQL_TINYINT = "TINYINT";
	public static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
	public static final String MYSQL_SMALLINT = "SMALLINT";
	public static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
	public static final String MYSQL_INT = "INT";
	public static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
	public static final String MYSQL_FLOAT = "FLOAT";
	public static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
	public static final String MYSQL_DOUBLE = "DOUBLE";
	public static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
	public static final String MYSQL_BIGINT = "BIGINT";
	public static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
	public static final String MYSQL_MEDIUMINT = "MEDIUMINT";
	public static final String MYSQL_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
	public static final String MYSQL_DATE = "DATE";
	public static final String MYSQL_TIME = "TIME";

	// utc time, unsupported by flink now.
	public static final String MYSQL_TIMESTAMP = "TIMESTAMP";

	public static final String MYSQL_DATETIME = "DATETIME";

	public static final String MYSQL_BLOB = "BLOB";
	public static final String MYSQL_TINYBLOB = "TINYBLOB";
	public static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";
	public static final String MYSQL_LONGBLOB = "LONGBLOB";
	// for blob text
	public static final String MYSQL_TEXT = "TEXT";
	public static final String MYSQL_VARCHAR = "VARCHAR";
	public static final String MYSQL_VARBINARY = "VARBINARY";
	public static final String MYSQL_CHAR = "CHAR";
	public static final String MYSQL_BINARY = "BINARY";

	// unsupported now.
	public static final String MYSQL_ENUM = "ENUM";
	// unsupported now.
	public static final String MYSQL_YEAR = "YEAR";
	// unsupported now.
	public static final String MYSQL_SET = "SET";
	// unsupported now.
	public static final String MYSQL_GEOMETRY = "GEOMETRY";

	// the following items are copy from document. these are not suppoted now.
	public static final String MYSQL_BOOL = "BOOL";
	public static final String MYSQL_BOOLEAN = "BOOLEAN";
	public static final String MYSQL_INTEGER = "INTEGER";
	public static final String MYSQL_DEC = "DEC";
	public static final String MYSQL_NUMERIC = "NUMERIC";
	public static final String MYSQL_FIXED = "FIXED";
	public static final String MYSQL_FLOAT_P = "FLOAT()";
	public static final String MYSQL_DOUBLE_PRECISION = "DOUBLE PRECISION";
	public static final String MYSQL_REAL = "REAL";
	public static final String MYSQL_TINYTEXT = "TINYTEXT";
	public static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
	public static final String MYSQL_LONGTEXT = "LONGTEXT";
	public static final String MYSQL_POINT = "POINT";
	public static final String MYSQL_LINESTRING = "LINESTRING";
	public static final String MYSQL_POLYGON = "POLYGON";
	public static final String MYSQL_MULTIPOINT = "MULTIPOINT";
	public static final String MYSQL_MULTILINESTRING = "MULTILINESTRING";
	public static final String MYSQL_MULTIPOLYGON = "MULTIPOLYGON";
	public static final String MYSQL_GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION";
	public static final String MYSQL_JSON = "JSON";

	private static DataType mysqlType2FlinkType(ResultSetMetaData metadata, int colIndex) throws SQLException {
		String mysqlType = metadata.getColumnTypeName(colIndex);

		int precision = metadata.getPrecision(colIndex);
		int scale = metadata.getScale(colIndex);

		switch (mysqlType) {
			case MYSQL_BIT:
			case MYSQL_BINARY:
				return DataTypes.BINARY(precision);
			case MYSQL_DECIMAL:
				if (precision > 0) {
					return DataTypes.DECIMAL(precision, scale);
				}
				return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18);
			case MYSQL_TINYINT:
				if (precision == 1) {
					return DataTypes.BOOLEAN();
				}
			case MYSQL_SMALLINT:
			case MYSQL_MEDIUMINT:
			case MYSQL_INT:
				return DataTypes.INT();
			case MYSQL_FLOAT:
				return DataTypes.FLOAT();
			case MYSQL_DOUBLE:
				return DataTypes.DOUBLE();
			case MYSQL_DATETIME:
			case MYSQL_TIMESTAMP:
				return DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class);
			case MYSQL_BIGINT:
				return DataTypes.BIGINT();
			case MYSQL_DATE:
				return DataTypes.DATE().bridgedTo(Date.class);
			case MYSQL_TIME:
				return DataTypes.TIME(scale).bridgedTo(Time.class);
			case MYSQL_VARCHAR:
				return DataTypes.VARCHAR(precision);
			case MYSQL_VARBINARY:
			case MYSQL_BLOB:
			case MYSQL_LONGBLOB:
			case MYSQL_MEDIUMBLOB:
				return DataTypes.VARBINARY(precision);
			case MYSQL_CHAR:
				return DataTypes.CHAR(precision);

			default:
				throw new UnsupportedOperationException(
					String.format("Doesn't support MySql type '%s' yet", mysqlType));
		}
	}

	private static String flinkType2MySqlColumnDefinition(DataType flinkType) {
		return flinkType.getLogicalType().accept(new MySqlFlink2ColumnDefinition());
	}

	private static class MySqlFlink2ColumnDefinition extends LogicalTypeDefaultVisitor <String> {

		@Override
		public String visit(VarBinaryType varBinaryType) {
			if (varBinaryType.getLength() > 16777215) {
				return String.format("%s", MYSQL_LONGBLOB);
			}
			if (varBinaryType.getLength() > 65535) {
				return String.format("%s", MYSQL_MEDIUMBLOB);
			}
			return varBinaryType.asSerializableString();
		}

		@Override
		public String visit(TinyIntType tinyIntType) {
			return tinyIntType.asSerializableString();
		}

		@Override
		public String visit(BooleanType booleanType) {
			return booleanType.asSerializableString();
		}

		@Override
		public String visit(SmallIntType smallIntType) {
			return smallIntType.asSerializableString();
		}

		@Override
		public String visit(IntType intType) {
			return intType.asSerializableString();
		}

		@Override
		public String visit(BigIntType bigIntType) {
			return bigIntType.asSerializableString();
		}

		@Override
		public String visit(DecimalType decimalType) {
			return decimalType.asSerializableString();
		}

		@Override
		public String visit(FloatType floatType) {
			return floatType.asSerializableString();
		}

		@Override
		public String visit(DoubleType doubleType) {
			return doubleType.asSerializableString();
		}

		@Override
		public String visit(DateType dateType) {
			return dateType.asSerializableString();
		}

		@Override
		public String visit(TimestampType timestampType) {
			return timestampType.asSerializableString();
		}

		@Override
		public String visit(TimeType timeType) {
			return timeType.asSerializableString();
		}

		@Override
		public String visit(CharType charType) {
			return charType.asSerializableString();
		}

		@Override
		public String visit(VarCharType varCharType) {

			//Ref: https://dev.mysql.com/doc/refman/8.0/en/string-type-syntax.html

			if (varCharType.getLength() > 16777215) {
				return MYSQL_LONGTEXT;
			}

			if (varCharType.getLength() > 65535) {
				return MYSQL_MEDIUMTEXT;
			}

			return varCharType.asSerializableString();
		}

		@Override
		public String visit(BinaryType binaryType) {
			return binaryType.asSerializableString();
		}

		@Override
		protected String defaultMethod(LogicalType logicalType) {

			if (logicalType.equals(LEGACY_DEC_DATA_TYPE.getLogicalType())) {
				return new DecimalType().asSerializableString();
			}

			throw new UnsupportedOperationException(
				"Could not convert the flink type " + logicalType.toString() + " to mysql type.");
		}
	}

	protected int flinkType2JdbcType(DataType flinkType) {
		return flinkType.getLogicalType().accept(new MySqlFlink2Jdbc());
	}

	@Override
	protected RichInputFormat <Row, InputSplit> createInputFormat(ObjectPath objectPath, TableSchema schema)
		throws Exception {

		return JDBCInputFormat.buildJDBCInputFormat()
			.setUsername(getParams().get(JdbcCatalogParams.USERNAME))
			.setPassword(getParams().get(JdbcCatalogParams.PASSWORD))
			.setDrivername(getParams().get(JdbcCatalogParams.DRIVER_NAME))
			.setDBUrl(rewriteDbUrl(getParams().get(JdbcCatalogParams.URL), objectPath))
			.setQuery("select * from " + rewriteObjectPath(objectPath).getFullName())
			.setRowTypeInfo(new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames()))
			.finish();
	}

	@Override
	protected OutputFormat <Row> createOutputFormat(ObjectPath objectPath, TableSchema schema, String sql) {
		return JDBCOutputFormat.buildJDBCOutputFormat()
			.setUsername(getParams().get(JdbcCatalogParams.USERNAME))
			.setPassword(getParams().get(JdbcCatalogParams.PASSWORD))
			.setDrivername(getParams().get(JdbcCatalogParams.DRIVER_NAME))
			.setDBUrl(rewriteDbUrl(getParams().get(JdbcCatalogParams.URL), objectPath))
			.setQuery(sql)
			.setSqlTypes(flinkTypes2JdbcTypes(schema.getFieldDataTypes()))
			.finish();
	}

	private static class MySqlFlink2Jdbc extends LogicalTypeDefaultVisitor <Integer> {

		@Override
		public Integer visit(BigIntType bigIntType) {
			return java.sql.Types.BIGINT;
		}

		@Override
		public Integer visit(VarCharType varCharType) {
			return java.sql.Types.VARCHAR;
		}

		@Override
		public Integer visit(DateType dateType) {
			return java.sql.Types.DATE;
		}

		@Override
		public Integer visit(DecimalType decimalType) {
			return java.sql.Types.DECIMAL;
		}

		@Override
		public Integer visit(DoubleType doubleType) {
			return java.sql.Types.DOUBLE;
		}

		@Override
		public Integer visit(FloatType floatType) {
			return java.sql.Types.REAL;
		}

		@Override
		public Integer visit(IntType intType) {
			return java.sql.Types.INTEGER;
		}

		@Override
		public Integer visit(SmallIntType smallIntType) {
			return java.sql.Types.SMALLINT;
		}

		@Override
		public Integer visit(TimeType timeType) {
			return java.sql.Types.TIME;
		}

		@Override
		public Integer visit(TimestampType timestampType) {
			return java.sql.Types.TIMESTAMP;
		}

		@Override
		public Integer visit(VarBinaryType varBinaryType) {
			return java.sql.Types.BINARY;
		}

		@Override
		public Integer visit(TinyIntType tinyIntType) {
			return java.sql.Types.TINYINT;
		}

		@Override
		public Integer visit(BooleanType booleanType) {
			return java.sql.Types.BOOLEAN;
		}

		@Override
		protected Integer defaultMethod(LogicalType logicalType) {
			if (logicalType.equals(LEGACY_DEC_DATA_TYPE.getLogicalType())) {
				return java.sql.Types.DECIMAL;
			}

			throw new UnsupportedOperationException(
				"Could not convert the flink type " + logicalType.toString() + " to jdbc type.");
		}
	}
}
