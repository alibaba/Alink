package com.alibaba.alink.common.io.catalog.sqlite;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.core.fs.Path;
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
import org.apache.flink.table.types.logical.BooleanType;
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
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.io.catalog.JdbcCatalog;
import com.alibaba.alink.params.io.JdbcCatalogParams;
import com.alibaba.alink.params.io.SqliteCatalogParams;
import com.alibaba.alink.params.io.shared.HasDefaultDatabase;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqliteCatalog extends JdbcCatalog {

	private static final String MAIN_DATABASE_SCHEMA = "main";

	public SqliteCatalog(
		String catalogName, String defaultDatabase,
		String[] dbUrls, String userName, String password) {

		this(
			new Params()
				.set(SqliteCatalogParams.DRIVER_NAME, "org.sqlite.JDBC")
				.set(SqliteCatalogParams.CATALOG_NAME, catalogName)
				.set(SqliteCatalogParams.DEFAULT_DATABASE,
					defaultDatabase == null ? parseDbNameFromUrl(dbUrls[0]) : defaultDatabase)
				.set(SqliteCatalogParams.URLS, dbUrls)
				.set(SqliteCatalogParams.USERNAME, userName)
				.set(SqliteCatalogParams.PASSWORD, password)
		);
	}

	public SqliteCatalog(Params params) {
		super(params);

		getParams().set(SqliteCatalogParams.URL,
			String.format("jdbc:sqlite:%s", getParams().get(SqliteCatalogParams.URLS)[findMainDatabase()]));
	}

	public static String parseDbNameFromUrl(String url) {
		String dbFilename = new Path(url).getName();
		int dotPos = dbFilename.lastIndexOf(".");
		return dbFilename.substring(0, dotPos < 0 ? dbFilename.length() : dotPos);
	}

	private int findMainDatabase() {
		return findDatabase(getParams().get(HasDefaultDatabase.DEFAULT_DATABASE));
	}

	private int findDatabase(String database) {
		String[] urls = getParams().get(SqliteCatalogParams.URLS);

		for (int i = 0; i < urls.length; ++i) {
			if (parseDbNameFromUrl(urls[i]).equals(database)) {
				return i;
			}
		}

		throw new IllegalArgumentException("Could not find the database.");
	}

	@Override
	public void open() throws CatalogException {
		try {
			Class.forName(getParams().get(SqliteCatalogParams.DRIVER_NAME));

			String[] urls = getParams().get(SqliteCatalogParams.URLS);

			Preconditions.checkState(urls != null && urls.length > 0);

			if (getParams().contains(SqliteCatalogParams.USERNAME)) {
				connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", urls[findMainDatabase()]));
			} else {
				connection = DriverManager.getConnection(
					String.format("jdbc:sqlite:%s", urls[findMainDatabase()]),
					getParams().get(SqliteCatalogParams.USERNAME),
					getParams().get(SqliteCatalogParams.PASSWORD)
				);
			}

			try (PreparedStatement preparedStatement = connection.prepareStatement("ATTACH DATABASE ? AS ?")) {
				int mainIndex = findMainDatabase();

				for (int i = 0; i < urls.length; ++i) {
					if (i == mainIndex) {
						continue;
					}

					preparedStatement.setString(0, urls[i]);
					preparedStatement.setString(1, parseDbNameFromUrl(urls[i]));

					preparedStatement.execute();
				}
			}

		} catch (ClassNotFoundException | SQLException ex) {
			throw new CatalogException(ex);
		}
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
		ArrayList <String> databases = new ArrayList <>();

		for (String dbUrl : getParams().get(SqliteCatalogParams.URLS)) {
			databases.add(parseDbNameFromUrl(dbUrl));
		}

		return databases;
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		return listDatabases().contains(databaseName);
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
		throw new UnsupportedOperationException();
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

		databaseName = databaseName.equals(getParams().get(HasDefaultDatabase.DEFAULT_DATABASE))
			? MAIN_DATABASE_SCHEMA : databaseName;

		try {
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet rs = meta.getTables(null, databaseName, null, new String[] {"TABLE"});
			ArrayList <String> tables = new ArrayList <>();
			while (rs.next()) {
				tables.add(rs.getString("TABLE_NAME"));
			}

			rs = meta.getTables(null, databaseName, null, new String[] {"VIEW"});
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

		tablePath = rewriteObjectPathInternal(tablePath);

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
					.append(
						flinkType2SqliteColumnDefinition(
							schema.getFieldDataType(i).orElseThrow(
								() -> new IllegalArgumentException("Counld not find the column.")
							)
						)
					);
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
					String.format("SELECT * FROM %s;",
						rewriteObjectPathInternal(tablePath).getFullName()));

				ResultSetMetaData rsmd = ps.getMetaData();

				String[] names = new String[rsmd.getColumnCount()];
				DataType[] types = new DataType[rsmd.getColumnCount()];

				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					names[i - 1] = rsmd.getColumnName(i);
					types[i - 1] = sqliteType2FlinkType(rsmd, i);
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
			executeSql(String.format("DROP TABLE %s", rewriteObjectPathInternal(tablePath).getFullName()));
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
			executeSql(String.format("ALTER TABLE %s RENAME TO %s", rewriteObjectPathInternal(tablePath).getFullName(),
				newTableName));
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

			databaseName = databaseName.equals(getParams().get(HasDefaultDatabase.DEFAULT_DATABASE))
				? MAIN_DATABASE_SCHEMA : databaseName;

			DatabaseMetaData meta = connection.getMetaData();
			ResultSet rs = meta.getTables(null, databaseName, null, new String[] {"VIEW"});
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
	 * Ref: https://www.sqlite.org/datatype3.html
	 */
	public static final String SQL_LITE_INT = "INT";
	public static final String SQL_LITE_INTEGER = "INTEGER";
	public static final String SQL_LITE_TINYINT = "TINYINT";
	public static final String SQL_LITE_SMALLINT = "SMALLINT";
	public static final String SQL_LITE_MEDIUMINT = "MEDIUMINT";
	public static final String SQL_LITE_BIGINT = "BIGINT";
	public static final String SQL_LITE_UNSIGNED_BIG_INT = "UNSIGNED BIG INT";
	public static final String SQL_LITE_INT2 = "INT2";
	public static final String SQL_LITE_INT8 = "INT8";
	public static final String SQL_LITE_CHARACTER = "CHARACTER(20)";
	public static final String SQL_LITE_VARCHAR = "VARCHAR";
	public static final String SQL_LITE_VARYING_CHARACTER = "VARYING CHARACTER(255)";
	public static final String SQL_LITE_NCHAR = "NCHAR(55)";
	public static final String SQL_LITE_NATIVE_CHARACTER = "NATIVE CHARACTER(70)";
	public static final String SQL_LITE_NVARCHAR = "NVARCHAR(100)";
	public static final String SQL_LITE_TEXT = "TEXT";
	public static final String SQL_LITE_CLOB = "CLOB";
	public static final String SQL_LITE_BLOB = "BLOB";
	public static final String SQL_LITE_NO_DATATYPE_SPECIFIED = "no datatype specified";
	public static final String SQL_LITE_REAL = "REAL";
	public static final String SQL_LITE_DOUBLE = "DOUBLE";
	public static final String SQL_LITE_DOUBLE_PRECISION = "DOUBLE PRECISION";
	public static final String SQL_LITE_FLOAT = "FLOAT";
	public static final String SQL_LITE_NUMERIC = "NUMERIC";
	public static final String SQL_LITE_DECIMAL = "DECIMAL";
	public static final String SQL_LITE_BOOLEAN = "BOOLEAN";
	public static final String SQL_LITE_DATE = "DATE";
	public static final String SQL_LITE_DATETIME = "DATETIME";
	public static final String SQL_LITE_VARBINARY = "VARBINARY";
	public static final String SQL_LITE_TIME = "TIME";
	public static final String SQL_LITE_TIMESTAMP = "TIMESTAMP";

	private static DataType sqliteType2FlinkType(ResultSetMetaData metadata, int colIndex) throws SQLException {
		String sqliteType = metadata.getColumnTypeName(colIndex);

		int precision = metadata.getPrecision(colIndex);
		int scale = metadata.getScale(colIndex);

		switch (sqliteType) {
			case SQL_LITE_INT2:
			case SQL_LITE_INT8:
			case SQL_LITE_INT:
			case SQL_LITE_INTEGER:
			case SQL_LITE_TINYINT:
			case SQL_LITE_SMALLINT:
			case SQL_LITE_BIGINT:
			case SQL_LITE_BOOLEAN:
			case SQL_LITE_DATE:
			case SQL_LITE_TIME:
			case SQL_LITE_TIMESTAMP:
				return DataTypes.INT();
			case SQL_LITE_NUMERIC:
			case SQL_LITE_DECIMAL:
				if (scale == 0) {
					return DataTypes.INT();
				} else {
					return DataTypes.DOUBLE();
				}
			case SQL_LITE_CHARACTER:
			case SQL_LITE_VARCHAR:
			case SQL_LITE_VARYING_CHARACTER:
			case SQL_LITE_NCHAR:
			case SQL_LITE_NATIVE_CHARACTER:
			case SQL_LITE_NVARCHAR:
			case SQL_LITE_TEXT:
				return DataTypes.STRING();
			case SQL_LITE_DOUBLE:
			case SQL_LITE_DOUBLE_PRECISION:
			case SQL_LITE_REAL:
			case SQL_LITE_FLOAT:
				return DataTypes.DOUBLE();
			case SQL_LITE_VARBINARY:
				return DataTypes.BYTES();
			default:
				throw new UnsupportedOperationException("Unsupported sqlite type: " + sqliteType);
		}
	}

	private static String flinkType2SqliteColumnDefinition(DataType flinkType) {
		return flinkType.getLogicalType().accept(new SqliteFlink2ColumnDefinition());
	}

	private static class SqliteFlink2ColumnDefinition extends LogicalTypeDefaultVisitor <String> {

		@Override
		public String visit(BigIntType bigIntType) {
			return bigIntType.asSerializableString();
		}

		@Override
		public String visit(VarCharType varCharType) {
			return varCharType.asSerializableString();
		}

		@Override
		public String visit(DateType dateType) {
			return dateType.asSerializableString();
		}

		@Override
		public String visit(DecimalType decimalType) {
			return decimalType.asSerializableString();
		}

		@Override
		public String visit(DoubleType doubleType) {
			return doubleType.asSerializableString();
		}

		@Override
		public String visit(FloatType floatType) {
			return floatType.asSerializableString();
		}

		@Override
		public String visit(IntType intType) {
			return intType.asSerializableString();
		}

		@Override
		public String visit(SmallIntType smallIntType) {
			return smallIntType.asSerializableString();
		}

		@Override
		public String visit(TimeType timeType) {
			return timeType.asSerializableString();
		}

		@Override
		public String visit(TimestampType timestampType) {
			return timestampType.asSerializableString();
		}

		@Override
		public String visit(VarBinaryType varBinaryType) {
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
		protected String defaultMethod(LogicalType logicalType) {
			if (logicalType.equals(LEGACY_DEC_DATA_TYPE.getLogicalType())) {
				return new DecimalType().asSerializableString();
			}

			throw new UnsupportedOperationException(
				"Could not convert the flink type " + logicalType.toString() + " to sqlite type.");
		}
	}

	protected int flinkType2JdbcType(DataType flinkType) {
		return flinkType.getLogicalType().accept(new SqliteFlink2Jdbc());
	}

	private static class SqliteFlink2Jdbc extends LogicalTypeDefaultVisitor <Integer> {

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

	@Override
	protected ObjectPath rewriteObjectPath(ObjectPath objectPath) {
		return new ObjectPath(MAIN_DATABASE_SCHEMA, objectPath.getObjectName());
	}

	protected ObjectPath rewriteObjectPathInternal(ObjectPath objectPath) {

		String databaseName = objectPath.getDatabaseName().equals(getParams().get(HasDefaultDatabase.DEFAULT_DATABASE))
			? MAIN_DATABASE_SCHEMA : objectPath.getDatabaseName();

		return new ObjectPath(databaseName, objectPath.getObjectName());
	}

	@Override
	protected String rewriteDbUrl(String url, ObjectPath objectPath) {
		return String.format(
			"jdbc:sqlite:%s",
			getParams().get(SqliteCatalogParams.URLS)[findDatabase(objectPath.getDatabaseName())]
		);
	}

	@Override
	protected RichInputFormat <Row, InputSplit> createInputFormat(ObjectPath objectPath, TableSchema schema)
		throws Exception {

		return JdbcInputFormat
			.buildJdbcInputFormat()
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
		return JdbcOutputFormat
			.buildJdbcOutputFormat()
			.setUsername(getParams().get(JdbcCatalogParams.USERNAME))
			.setPassword(getParams().get(JdbcCatalogParams.PASSWORD))
			.setDrivername(getParams().get(JdbcCatalogParams.DRIVER_NAME))
			.setDBUrl(rewriteDbUrl(getParams().get(JdbcCatalogParams.URL), objectPath))
			.setQuery(sql)
			.setSqlTypes(flinkTypes2JdbcTypes(schema.getFieldDataTypes()))
			.finish();
	}
}
