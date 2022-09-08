package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.operator.common.io.types.JdbcTypeConverter;
import com.alibaba.alink.operator.local.sql.CalciteFunctionCompiler;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.alibaba.alink.operator.common.sql.CalciteSelectMapper.registerFlinkBuiltInFunctions;

/**
 * Execute SQL on MTables with local Calcite engine, which is similar to {@link BatchSqlOperators}.
 */
public class MTableCalciteSqlExecutor implements SqlExecutor <MTable> {
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
		registerFlinkBuiltInFunctions(rootSchema);
		calciteSchema = rootSchema.unwrap(CalciteSchema.class);
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

	@Override
	public MTable query(String sql) {
		try (TemporaryClassLoaderContext context =
				 TemporaryClassLoaderContext.of(calciteFunctionCompiler.getClassLoader())) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			ResultSetMetaData metaData = resultSet.getMetaData();

			int numCols = metaData.getColumnCount();
			String[] colNames = new String[numCols];
			TypeInformation <?>[] colTypes = new TypeInformation[numCols];
			for (int i = 0; i < numCols; i += 1) {
				colNames[i] = metaData.getColumnLabel(i + 1);
				colTypes[i] = JdbcTypeConverter.getFlinkType(metaData.getColumnType(i + 1));
			}
			List <Row> data = new ArrayList <>();
			while (resultSet.next()) {
				Row row = new Row(numCols);
				for (int i = 0; i < numCols; i += 1) {
					row.setField(i, resultSet.getObject(i + 1));
				}
				data.add(row);
			}
			return new MTable(data, colNames, colTypes);
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
