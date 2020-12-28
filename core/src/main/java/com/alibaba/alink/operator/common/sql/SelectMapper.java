package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.SelectBatchOp;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.sql.functions.MathFunctions;
import com.alibaba.alink.operator.common.sql.functions.StringFunctions;
import com.alibaba.alink.params.sql.SelectParams;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.function.BiConsumer;

/**
 * Execute the sql select operation without Flink.
 */
public class SelectMapper extends Mapper {

	private static final long serialVersionUID = 6207092249511500058L;
	private TableSchema outputSchema;
	private Connection connection;
	private PreparedStatement preparedStatement;

	private final static String TEMPLATE = "SELECT %s FROM (SELECT %s FROM (VALUES (1))) foo";

	/**
	 * Constructor.
	 *
	 * @param dataSchema input table schema.
	 * @param params     input parameters.
	 */
	public SelectMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	public static void registerFlinkBuiltInFunctions(SchemaPlus schema) {
		BiConsumer <String, Method> addScalarFunctionConsumer =
			(k, v) -> schema.add(k, ScalarFunctionImpl.create(v));

		addScalarFunctionConsumer.accept("LOG2", MathFunctions.LOG2);
		addScalarFunctionConsumer.accept("LOG2", MathFunctions.LOG2_DEC);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG_DEC);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG_WITH_BASE);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG_WITH_BASE_DEC_DOU);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG_WITH_BASE_DOU_DEC);
		addScalarFunctionConsumer.accept("LOG", MathFunctions.LOG_WITH_BASE_DEC_DEC);
		addScalarFunctionConsumer.accept("SINH", MathFunctions.SINH);
		addScalarFunctionConsumer.accept("SINH", MathFunctions.SINH_DEC);
		addScalarFunctionConsumer.accept("COSH", MathFunctions.COSH);
		addScalarFunctionConsumer.accept("COSH", MathFunctions.COSH_DEC);
		addScalarFunctionConsumer.accept("TANH", MathFunctions.TANH);
		addScalarFunctionConsumer.accept("TANH", MathFunctions.TANH_DEC);
		addScalarFunctionConsumer.accept("UUID", MathFunctions.UUID);
		addScalarFunctionConsumer.accept("BIN", MathFunctions.BIN);
		addScalarFunctionConsumer.accept("HEX", MathFunctions.HEX_LONG);
		addScalarFunctionConsumer.accept("HEX", MathFunctions.HEX_STRING);

		addScalarFunctionConsumer.accept("FROM_BASE64", StringFunctions.FROMBASE64);
		addScalarFunctionConsumer.accept("TO_BASE64", StringFunctions.TOBASE64);
		addScalarFunctionConsumer.accept("LPAD", StringFunctions.LPAD);
		addScalarFunctionConsumer.accept("RPAD", StringFunctions.RPAD);
		//addScalarFunctionConsumer.accept("REGEXP_REPLACE", StringFunctions.REGEXP_REPLACE);
		addScalarFunctionConsumer.accept("REGEXP_EXTRACT", StringFunctions.REGEXP_EXTRACT);

		addScalarFunctionConsumer.accept("LTRIM", BuiltInMethod.LTRIM.method);
		addScalarFunctionConsumer.accept("RTRIM", BuiltInMethod.RTRIM.method);

		addScalarFunctionConsumer.accept("MD5", StringFunctions.MD5);
		addScalarFunctionConsumer.accept("SHA1", StringFunctions.SHA1);
		addScalarFunctionConsumer.accept("SHA224", StringFunctions.SHA224);
		addScalarFunctionConsumer.accept("SHA256", StringFunctions.SHA256);
		addScalarFunctionConsumer.accept("SHA384", StringFunctions.SHA384);
		addScalarFunctionConsumer.accept("SHA512", StringFunctions.SHA512);
		addScalarFunctionConsumer.accept("SHA2", StringFunctions.SHA2);
	}

	@Override
	public void open() {
		super.open();
		try {
			Class.forName("org.apache.calcite.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		CalciteConnection calciteConnection;
		try {
			connection = DriverManager.getConnection("jdbc:calcite:fun=mysql");
			calciteConnection = connection.unwrap(CalciteConnection.class);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		SchemaPlus rootSchema = calciteConnection.getRootSchema();
		registerFlinkBuiltInFunctions(rootSchema);

		TableSchema dataSchema = getDataSchema();
		String clause = params.get(SelectParams.CLAUSE);

		MemSourceBatchOp source = new MemSourceBatchOp(Collections.emptyList(), dataSchema);
		outputSchema = source.linkTo(new SelectBatchOp().setClause(clause)).getSchema();

		TypeInformation <?>[] fieldTypes = dataSchema.getFieldTypes();
		String[] fieldNames = dataSchema.getFieldNames();
		int fieldCount = fieldNames.length;

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < fieldCount; i += 1) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append("CAST(? as ");
			sb.append(FlinkTypeConverter.getTypeString(fieldTypes[i]));
			sb.append(") as ");
			sb.append(fieldNames[i]);
		}

		String query = String.format(TEMPLATE, clause, sb);
		try {
			preparedStatement = calciteConnection.prepareStatement(query);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		super.close();
		try {
			connection.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TableSchema getOutputSchema() {
		return outputSchema;
	}

	@Override
	public Row map(Row row) throws Exception {
		for (int i = 0; i < row.getArity(); i += 1) {
			preparedStatement.setObject(i + 1, row.getField(i));
		}
		try (ResultSet resultSet = preparedStatement.executeQuery()) {
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			Object[] values = new Object[columnCount];
			if (resultSet.next()) {
				for (int i = 0; i < columnCount; i += 1) {
					values[i] = resultSet.getObject(i + 1);
				}
			}
			return Row.of(values);
		}
	}

}
