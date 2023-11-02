package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.common.sql.builtin.time.DataFormat;
import com.alibaba.alink.common.sql.builtin.time.FromUnixTime;
import com.alibaba.alink.common.sql.builtin.time.Now;
import com.alibaba.alink.common.sql.builtin.time.ToTimeStamp;
import com.alibaba.alink.common.sql.builtin.time.ToTimeStampFromFormat;
import com.alibaba.alink.common.sql.builtin.time.ToTimeStampMicro;
import com.alibaba.alink.common.sql.builtin.time.UnixTimeStamp;
import com.alibaba.alink.common.sql.builtin.time.UnixTimeStampMicro;
import com.alibaba.alink.operator.batch.sql.BatchSqlOperators;
import com.alibaba.alink.operator.common.io.types.JdbcTypeConverter;
import com.alibaba.alink.operator.local.sql.CalciteFunctionCompiler;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare.CalciteSignature;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl.JavaType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Execute SQL on MTables with local Calcite engine, which is similar to {@link BatchSqlOperators}.
 */
public class MTableCalciteSqlExecutor implements SqlExecutor <MTable> {

	private static final Logger LOG = LoggerFactory.getLogger(MTableCalciteSqlExecutor.class);

	private final Connection connection;
	private final SchemaPlus rootSchema;
	private final CalciteSchema calciteSchema;
	private final CalciteFunctionCompiler calciteFunctionCompiler;

	public MTableCalciteSqlExecutor(LocalMLEnvironment env) {
		calciteFunctionCompiler = env.getCalciteFunctionCompiler();
		/*
		In EAS, threads started have no contextClassLoader, which makes {@link
		CompilerFactoryFactory#getDefaultCompilerFactory} failed. So we manually set it to the classloader of this
		class.
		 */
		try {
			Class.forName("org.apache.calcite.jdbc.Driver");
			Properties info = new Properties();
			info.setProperty(CalciteConnectionProperty.DEFAULT_NULL_COLLATION.camelName(), NullCollation.LAST.name());
			info.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "true");
			info.setProperty(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.name());
			info.setProperty(CalciteConnectionProperty.QUOTING.camelName(), Quoting.BACK_TICK.name());
			connection = DriverManager.getConnection("jdbc:calcite:fun=mysql", info);
		} catch (ClassNotFoundException | SQLException e) {
			throw new AkUnclassifiedErrorException("Failed to initialize JDBC connection.", e);
		}

		CalciteConnection calciteConnection;
		try {
			calciteConnection = connection.unwrap(CalciteConnection.class);
		} catch (SQLException e) {
			throw new AkIllegalStateException("Failed to unwrap CalciteConnection instance.", e);
		}
		rootSchema = calciteConnection.getRootSchema();
		CalciteSelectMapper.registerFlinkBuiltInFunctions(rootSchema);
		registerUdf();

		calciteSchema = rootSchema.unwrap(CalciteSchema.class);
	}

	private void registerUdf() {
		addFunction("now", new Now());
		addFunction("to_timestamp", new ToTimeStamp());
		addFunction("unix_timestamp", new UnixTimeStamp());
		addFunction("from_unixtime", new FromUnixTime());
		addFunction("date_format_ltz", new DataFormat());
		addFunction("unix_timestamp_macro", new UnixTimeStampMicro());
		addFunction("to_timestamp_micro", new ToTimeStampMicro());
		addFunction("to_timestamp_from_format", new ToTimeStampFromFormat());
	}

	@Override
	public String[] listTableNames() {
		return calciteSchema.getTableNames().toArray(new String[0]);
	}

	@Override
	public String[] listFunctionNames() {
		return calciteSchema.getFunctionNames().toArray(new String[0]);
	}

	@Override
	public void addTable(String name, MTable t) {
		calciteSchema.add(name, new MTableTable(t));
	}

	@Override
	public void removeTable(String name) {
		calciteSchema.removeTable(name);
	}

	@Override
	public void addFunction(String name, ScalarFunction function) {
		rootSchema.add(name, ScalarFunctionImpl.create(function.getClass(), "eval"));
	}

	@Override
	public void addFunction(String name, TableFunction <Row> function) {
		final int MAX_NUM_PARAMETERS_FOR_VARARG_FUNCTION = 16;
		// only for function with parameters: Object...
		for (int i = 0; i < MAX_NUM_PARAMETERS_FOR_VARARG_FUNCTION; i += 1) {
			//noinspection unchecked
			org.apache.calcite.schema.TableFunction calciteTableFunction =
				calciteFunctionCompiler.getCalciteTableFunction("Calcite" + name + i,
					i, (Class <TableFunction <Row>>) function.getClass());
			rootSchema.add(name, calciteTableFunction);
		}
	}

	private TableSchema extractSchema(ResultSetMetaData metaData) throws SQLException {
		int numCols = metaData.getColumnCount();
		String[] colNames = new String[numCols];
		TypeInformation <?>[] colTypes = new TypeInformation[numCols];
		for (int i = 0; i < numCols; i += 1) {
			colNames[i] = metaData.getColumnLabel(i + 1);
			colTypes[i] = JdbcTypeConverter.getFlinkType(metaData.getColumnType(i + 1));
		}
		//noinspection deprecation
		return new TableSchema(colNames, colTypes);
	}

	/**
	 * When user-defined types are in the results, {@link ResultSetMetaData#getColumnType} returns {@link Types},
	 * which makes Alink unable to get the right {@link TypeInformation} for these fields.
	 * <p>
	 * To obtain right {@link TypeInformation} for these fields, we have to use reflections to access private field of
	 * {@link ResultSetMetaData}. As a fallback, legacy method is still used when reflections cannot work.
	 */
	private TableSchema extractSchemaByReflection(ResultSetMetaData metaData) throws SQLException {
		try {
			int numCols = metaData.getColumnCount();
			String[] colNames = new String[numCols];
			TypeInformation <?>[] colTypes = new TypeInformation[numCols];

			AvaticaResultSetMetaData avaticaResultSetMetaData = metaData.unwrap(AvaticaResultSetMetaData.class);
			Field signatureField = AvaticaResultSetMetaData.class.getDeclaredField("signature");
			signatureField.setAccessible(true);
			CalciteSignature <?> signature = (CalciteSignature <?>) signatureField.get(avaticaResultSetMetaData);
			RelDataType rowType = signature.rowType;
			List <RelDataTypeField> fields = rowType.getFieldList();

			for (int i = 0; i < fields.size(); i++) {
				colNames[i] = fields.get(i).getName();
				RelDataType relDataType = fields.get(i).getType();
				boolean isUdt = false;
				if (relDataType instanceof JavaType) {
					JavaType javaType = (JavaType) relDataType;
					Class <?> clazz = javaType.getJavaClass();
					if (clazz.getCanonicalName().startsWith("com.alibaba.alink.")) {
						colTypes[i] = TypeInformation.of(clazz);
						isUdt = true;
					}
				}
				if (!isUdt) {
					colTypes[i] = JdbcTypeConverter.getFlinkType(metaData.getColumnType(i + 1));
				}
			}
			//noinspection deprecation
			return new TableSchema(colNames, colTypes);
		} catch (Exception ignored) {
			LOG.info("Failed to extract schema from meta data by reflection, so fallback to the legacy approach: "
				+ metaData.toString());
			return extractSchema(metaData);
		}
	}

	@Override
	public MTable query(String sql) {
		try (TemporaryClassLoaderContext ignored =
				 TemporaryClassLoaderContext.of(calciteFunctionCompiler.getClassLoader())) {
			Statement statement = connection.createStatement();

			// todo: select col1 where unix_timestamp_macro(col1)=12 will exception.
			ResultSet resultSet = statement.executeQuery(sql);
			ResultSetMetaData metaData = resultSet.getMetaData();

			TableSchema schema = extractSchemaByReflection(metaData);
			int numCols = metaData.getColumnCount();

			// something wrong when timestamp type input and output in calcite. need convert to long in execute
			// query, and convert to timestamp when output.
			boolean isHasTimeStamp = false;
			StringBuilder sbd = new StringBuilder();
			for (int i = 0; i < numCols; i++) {
				sbd.append(",");
				String colName = schema.getFieldName(i).get();
				if (Types.SQL_TIMESTAMP == schema.getFieldType(i).get()) {
					sbd.append(String.format("unix_timestamp_macro(%s) as %s", colName, colName));
					isHasTimeStamp = true;
				} else {
					sbd.append(colName);
				}
			}

			if (isHasTimeStamp) {
				String newQuery = "select " + sbd.substring(1) + " from "
					+ "\n("
					+ sql
					+ "\n)";
				resultSet = statement.executeQuery(newQuery);
			}

			List <Row> data = new ArrayList <>();

			while (resultSet.next()) {
				Row row = new Row(numCols);
				for (int i = 0; i < numCols; i += 1) {
					if (Types.SQL_TIMESTAMP == schema.getFieldType(i).get()) {
						Object tmp = resultSet.getObject(i + 1);
						if (tmp instanceof Long) {
							row.setField(i, new Timestamp((long)tmp));
						} else {
							row.setField(i, tmp);
						}
					} else {
						row.setField(i, resultSet.getObject(i + 1));
					}
				}
				data.add(row);
			}

			return new MTable(data, schema);
		} catch (SQLException e) {
			throw new AkUnclassifiedErrorException("Failed to execute query: " + sql, e);
		}

	}

	@Override
	public MTable as(MTable t, String fields) {
		ArrayList <Row> rows = new ArrayList <>(t.getRows().size());
		for (Row r : t.getRows()) {
			rows.add(Row.copy(r));
		}
		final Set <Character> QUOTES = new HashSet <>(Arrays.asList('\'', '\"', '`'));
		String[] colNames = Arrays.stream(fields.split(","))
			.map(String::trim)
			.map(d -> ((d.length() >= 2) && (QUOTES.contains(d.charAt(0))) && (d.charAt(0) == d.charAt(d.length() - 1)))
				? d.substring(1, d.length() - 1)
				: d)
			.toArray(String[]::new);
		return new MTable(rows, colNames, t.getColTypes());
	}
}
