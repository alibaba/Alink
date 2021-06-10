package com.alibaba.alink.common.io.catalog.derby;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
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
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.catalog.JdbcCatalog;
import com.alibaba.alink.params.io.DerbyDBParams;
import com.alibaba.alink.params.io.JdbcCatalogParams;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DerbyCatalog extends JdbcCatalog {

	private static final JdbcDialect DERBY_DIALECT = JdbcDialects
		.get("jdbc:derby:")
		.orElseThrow(
			() -> new IllegalArgumentException("Could not find the derby dialect.")
		);

	public static final String DEFAULT_DATABASE_SCHEMA = "APP";

	public DerbyCatalog(
		String catalogName, String defaultDatabase, String derbyPath,
		String userName, String password) {

		this(
			new Params()
				.set(JdbcCatalogParams.DRIVER_NAME,
					DERBY_DIALECT.defaultDriverName().orElse("org.apache.derby.jdbc.EmbeddedDriver"))
				.set(JdbcCatalogParams.CATALOG_NAME, catalogName)
				.set(JdbcCatalogParams.DEFAULT_DATABASE,
					defaultDatabase == null ? DEFAULT_DATABASE_SCHEMA : defaultDatabase)
				.set(JdbcCatalogParams.URL, String.format("jdbc:derby:%s;create=true", derbyPath))
				.set(JdbcCatalogParams.USERNAME, userName)
				.set(JdbcCatalogParams.PASSWORD, password)
		);
	}

	public DerbyCatalog(Params params) {
		super(params);
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName)
		throws DatabaseNotExistException, CatalogException {

		databaseName = databaseName.toUpperCase();

		if (databaseExists(databaseName)) {
			return new CatalogDatabaseImpl(Collections.emptyMap(), null);
		} else {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
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

	@Override
	public List <String> listDatabases() throws CatalogException {
		try {
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet rs = meta.getSchemas();
			ArrayList <String> schemas = new ArrayList <>();
			while (rs.next()) {
				schemas.add(rs.getString(1));
			}
			return schemas;
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

			String userName = getParams().get(DerbyDBParams.USERNAME);
			name = name.toUpperCase();

			if (userName == null) {
				executeSql(String.format("CREATE SCHEMA %s", name));
			} else {
				executeSql(String.format("CREATE SCHEMA %s AUTHORIZATION %s", name, userName));
			}
		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		return listDatabases().contains(databaseName.toUpperCase());
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {

		boolean databaseExists = databaseExists(name);

		if (ignoreIfNotExists && !databaseExists) {
			return;
		} else if (!ignoreIfNotExists && !databaseExists) {
			throw new DatabaseNotExistException(getName(), name);
		}

		try {
			executeSql(String.format("DROP SCHEMA %s RESTRICT", name.toUpperCase()));
		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	/**
	 * Renamed the schema in derby is not supported.
	 * <p>
	 * Ref: http://apache-database.10148.n7.nabble.com/Rename-Schema-td95708.html
	 */
	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public List <String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		databaseName = databaseName.toUpperCase();

		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}

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
			tables = listTables(tablePath.getDatabaseName().toUpperCase());
		} catch (DatabaseNotExistException e) {
			return false;
		}

		return tables.contains(tablePath.getObjectName().toUpperCase());
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

		tablePath = new ObjectPath(tablePath.getDatabaseName().toUpperCase(), tablePath.getObjectName().toUpperCase());

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

				String colName = schema
					.getFieldName(i)
					.orElseThrow(() -> new IllegalArgumentException("Could not find the column in derby."))
					.toUpperCase();

				DataType colType = schema
					.getFieldDataType(i)
					.orElseThrow(() -> new IllegalArgumentException("Could not find the column in derby."));

				String columnDefinition = flinkType2DerbyColumnDefinition(colType);

				sbd.append(colName).append(" ").append(columnDefinition);
			}

			sbd.append(")");

			executeSql(sbd.toString());

		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		tablePath = new ObjectPath(tablePath.getDatabaseName().toUpperCase(), tablePath.getObjectName().toUpperCase());

		if (tableExists(tablePath)) {
			try {

				PreparedStatement ps = connection.prepareStatement(
					String.format("SELECT * FROM %s", tablePath.getFullName()));

				ResultSetMetaData rsmd = ps.getMetaData();

				String[] names = new String[rsmd.getColumnCount()];
				DataType[] types = new DataType[rsmd.getColumnCount()];

				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					names[i - 1] = rsmd.getColumnName(i);
					types[i - 1] = derbyType2FlinkType(rsmd, i);
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
		tablePath = new ObjectPath(tablePath.getDatabaseName().toUpperCase(), tablePath.getObjectName().toUpperCase());

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

		tablePath = new ObjectPath(tablePath.getDatabaseName().toUpperCase(), tablePath.getObjectName().toUpperCase());

		boolean tableExists = tableExists(tablePath);

		if (ignoreIfNotExists && !tableExists) {
			return;
		} else if (!ignoreIfNotExists && !tableExists) {
			throw new TableNotExistException(getName(), tablePath);
		}

		try {
			executeSql(String.format("RENAME TABLE %s TO %s", tablePath.getFullName(), newTableName));
		} catch (SQLException ex) {
			throw new CatalogException(ex);
		}
	}

	/**
	 * Ref: <a href="https://db.apache.org/derby/docs/10.2/ref/rrefsqlj81859.html#rrefsqlj81859">derby alter table
	 * statement</a>
	 **/
	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public List <String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		try {
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet rs = meta.getTables(null, databaseName.toUpperCase(), null, new String[] {"VIEW"});
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
	 * Compatible with more type.
	 * <p>
	 * Warning: These will cause that the schema of the table created by {@link DerbyCatalog#createTable(ObjectPath,
	 * CatalogBaseTable, boolean)} is different to the {@link CatalogBaseTable} returned by {@link
	 * DerbyCatalog#getTable(ObjectPath)}
	 * <p>
	 *
	 * @see <a href="https://db.apache.org/derby/docs/10.1/ref/crefsqlj31068.html">derby type</a>
	 * <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/connectors/jdbc.html#data-type-mapping">flink
	 * data type mapping</a>
	 */

	public static final String DERBY_BIGINT = "BIGINT";
	public static final String DERBY_DATE = "DATE";

	/**
	 * Java type: java.math.BigDecimal. JDBC type: DECIMAL.
	 **/
	public static final String DERBY_DECIMAL = "DECIMAL";

	public static final String DERBY_DOUBLE = "DOUBLE";
	public static final String DERBY_INTEGER = "INTEGER";
	public static final String DERBY_REAL = "REAL";
	public static final String DERBY_SMALLINT = "SMALLINT";
	public static final String DERBY_TIME = "TIME";
	public static final String DERBY_TIMESTAMP = "TIMESTAMP";
	public static final String DERBY_VARCHAR = "VARCHAR";

	// synonymously with DOUBLE.
	public static final String DERBY_DOUBLE_PRECISION = "DOUBLE PRECISION";

	/**
	 * synonymously with DECIMAL. Java type: java.math.BigDecimal. JDBC type: NUMERIC.
	 **/
	public static final String DERBY_NUMERIC = "NUMERIC";

	/**
	 * Java type: java.lang.String. JDBC type: CHAR.
	 */
	public static final String DERBY_CHAR = "CHAR";

	/**
	 * Dynamic type. Ref: https://db.apache.org/derby/docs/10.1/ref/rrefsqlj27281.html
	 */
	public static final String DERBY_FLOAT = "FLOAT";

	/**
	 * JDBC type: BINARY
	 */
	public static final String DERBY_CHAR_FOR_BIT_DATA = "CHAR () FOR BIT DATA";

	/**
	 * JDBC type: VARBINARY
	 */
	public static final String DERBY_VARCHAR_FOR_BIT_DATA = "VARCHAR () FOR BIT DATA";

	/**
	 * Java type: java.lang.String. JDBC type: LONGVARCHAR.
	 */
	public static final String DERBY_LONG_VARCHAR = "LONG VARCHAR";

	/**
	 * Could not support now. No java type or jdbc type.
	 */
	public static final String DERBY_LONG_VARCHAR_FOR_BIT_DATA = "LONG VARCHAR FOR BIT DATA";

	/**
	 * Could not support now.
	 * <p>
	 * Note: org.apache.derby.impl.jdbc.EmbedBlob could not be deserialized by flink type system. Need to develop the
	 * {@link org.apache.flink.connector.jdbc.JdbcInputFormat} afresh.
	 */
	public static final String DERBY_BLOB = "BLOB";
	public static final TypeInformation <Blob> BLOB = TypeInformation.of(Blob.class);

	/**
	 * Could not support now.
	 * <p>
	 * Note: org.apache.derby.impl.jdbc.EmbedClob could not be deserialized by flink type system. Need to develop the
	 * {@link org.apache.flink.connector.jdbc.JdbcInputFormat} afresh.
	 */
	public static final String DERBY_CLOB = "CLOB";
	public static final TypeInformation <Clob> CLOB = TypeInformation.of(Clob.class);

	private static DataType derbyType2FlinkType(ResultSetMetaData metadata, int colIndex) throws SQLException {
		String derbyType = metadata.getColumnTypeName(colIndex);

		int precision = metadata.getPrecision(colIndex);
		int scale = metadata.getScale(colIndex);

		switch (derbyType) {
			case DERBY_BIGINT:
				return DataTypes.BIGINT();
			case DERBY_CHAR:
				return DataTypes.CHAR(precision);
			case DERBY_LONG_VARCHAR:
			case DERBY_VARCHAR:
				return DataTypes.VARCHAR(precision);
			case DERBY_LONG_VARCHAR_FOR_BIT_DATA:
			case DERBY_VARCHAR_FOR_BIT_DATA:
				return DataTypes.BYTES().bridgedTo(byte[].class);
			case DERBY_CHAR_FOR_BIT_DATA:
				return DataTypes.BINARY(precision);
			case DERBY_DATE:
				return DataTypes.DATE().bridgedTo(Date.class);
			case DERBY_DECIMAL:
			case DERBY_NUMERIC:
				return DataTypes.DECIMAL(precision, scale);
			case DERBY_DOUBLE:
			case DERBY_DOUBLE_PRECISION:
				return DataTypes.DOUBLE();
			case DERBY_FLOAT:
				// Ref: https://db.apache.org/derby/docs/10.1/ref/rrefsqlj27281.html
				if (precision > 23) {
					return DataTypes.DOUBLE();
				} else {
					return DataTypes.FLOAT();
				}
			case DERBY_REAL:
				return DataTypes.FLOAT();
			case DERBY_INTEGER:
			case DERBY_SMALLINT:
				//jdbc of derby return int when the column type is smallint.
				return DataTypes.INT();
			case DERBY_TIME:
				return DataTypes.TIME(scale).bridgedTo(Time.class);
			case DERBY_TIMESTAMP:
				// derby timestamp use 6 precision
				// but if we use 6 precision, the "org.apache.flink.table.api.TableException: Unsupported conversion
				// from data type 'TIMESTAMP(6)' (conversion class: java.sql.Timestamp) to type information. Only data
				// types that originated from type information fully support a reverse conversion."
				// will be thrown.
				// We use 3-precision by default and only support 3-precision in derby.
				return DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class);
			default:
				throw new UnsupportedOperationException("Unsupported derby type: " + derbyType);
		}
	}

	private static String flinkType2DerbyColumnDefinition(DataType flinkType) {
		return flinkType.getLogicalType().accept(new DerbyFlink2ColumnDefinition());
	}

	private static class DerbyFlink2ColumnDefinition extends LogicalTypeDefaultVisitor <String> {

		@Override
		public String visit(BigIntType bigIntType) {
			return DERBY_BIGINT;
		}

		@Override
		public String visit(CharType charType) {
			return charType.asSerializableString();
		}

		@Override
		public String visit(VarCharType varCharType) {
			if (varCharType.getLength() > 32700) {
				// Ref: https://db.apache.org/derby/docs/10.1/ref/rrefsqlj15147.html
				// derby could not support it. we determined until runtime. return long varchar by default.
				//throw new IllegalArgumentException(
				//	varCharType.asSummaryString() + " is not supported by derby"
				//);
				return DERBY_LONG_VARCHAR;
			} else if (varCharType.getLength() > 32672) {
				return DERBY_LONG_VARCHAR;
			} else {
				return varCharType.asSerializableString();
			}
		}

		@Override
		public String visit(DateType dateType) {
			return DERBY_DATE;
		}

		@Override
		public String visit(DecimalType decimalType) {
			return decimalType.asSerializableString();
		}

		@Override
		public String visit(DoubleType doubleType) {
			return DERBY_DOUBLE;
		}

		@Override
		public String visit(FloatType floatType) {
			return DERBY_REAL;
		}

		@Override
		public String visit(IntType intType) {
			return DERBY_INTEGER;
		}

		@Override
		public String visit(TimeType timeType) {
			return DERBY_TIME;
		}

		@Override
		public String visit(TimestampType timestampType) {
			return DERBY_TIMESTAMP;
		}

		@Override
		public String visit(SmallIntType smallIntType) {
			return DERBY_SMALLINT;
		}

		@Override
		public String visit(VarBinaryType varBinaryType) {
			if (varBinaryType.getLength() > 32672) {
				return DERBY_LONG_VARCHAR_FOR_BIT_DATA;
			} else {
				return "VARCHAR (" + varBinaryType.getLength() + ") FOR BIT DATA";
			}
		}

		@Override
		public String visit(BinaryType binaryType) {
			if (binaryType.getLength() > 254) {
				throw new IllegalArgumentException(
					binaryType.asSummaryString() + " is not supported by derby"
				);
			}
			return "CHAR (" + binaryType.getLength() + ") FOR BIT DATA";
		}

		@Override
		protected String defaultMethod(LogicalType logicalType) {

			if (logicalType.equals(LEGACY_DEC_DATA_TYPE.getLogicalType())) {
				return new DecimalType().asSerializableString();
			}

			throw new UnsupportedOperationException(
				"Could not convert the flink type " + logicalType.toString() + " to derby type.");
		}
	}

	@Override
	protected int flinkType2JdbcType(DataType flinkType) {

		return flinkType.getLogicalType().accept(new DerbyFlink2Jdbc());
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

	private static class DerbyFlink2Jdbc extends LogicalTypeDefaultVisitor <Integer> {

		@Override
		public Integer visit(BigIntType bigIntType) {
			return Types.BIGINT;
		}

		@Override
		public Integer visit(CharType charType) {
			return Types.CHAR;
		}

		@Override
		public Integer visit(VarCharType varCharType) {
			return Types.VARCHAR;
		}

		@Override
		public Integer visit(VarBinaryType varBinaryType) {
			return Types.VARBINARY;
		}

		@Override
		public Integer visit(BinaryType binaryType) {
			return Types.BINARY;
		}

		@Override
		public Integer visit(DateType dateType) {
			return Types.DATE;
		}

		@Override
		public Integer visit(DecimalType decimalType) {
			return Types.DECIMAL;
		}

		@Override
		public Integer visit(DoubleType doubleType) {
			return Types.DOUBLE;
		}

		@Override
		public Integer visit(FloatType floatType) {
			return Types.REAL;
		}

		@Override
		public Integer visit(IntType intType) {
			return Types.INTEGER;
		}

		@Override
		public Integer visit(TimeType timeType) {
			return Types.TIME;
		}

		@Override
		public Integer visit(TimestampType timestampType) {
			return Types.TIMESTAMP;
		}

		@Override
		public Integer visit(SmallIntType smallIntType) {
			return Types.SMALLINT;
		}

		@Override
		protected Integer defaultMethod(LogicalType logicalType) {
			if (logicalType.equals(LEGACY_DEC_DATA_TYPE.getLogicalType())) {
				return Types.DECIMAL;
			}

			throw new UnsupportedOperationException(
				"Could not convert the flink type " + logicalType.toString() + " to jdbc type.");
		}
	}
}
