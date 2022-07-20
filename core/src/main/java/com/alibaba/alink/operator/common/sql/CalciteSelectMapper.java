package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.exceptions.AkParseErrorException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Execute the sql select operation without Flink.
 */
public class CalciteSelectMapper extends Mapper {

	private static final long serialVersionUID = 6207092249511500058L;

	private final ConcurrentHashMap <Thread, Connection> threadConnectionMap = new ConcurrentHashMap <>();
	private final ConcurrentHashMap <Thread, PreparedStatement> threadPreparedStatementMap =
		new ConcurrentHashMap <>();

	private final static String TEMPLATE = "SELECT %s FROM (SELECT %s FROM (VALUES (1))) foo";

	/**
	 * Constructor.
	 *
	 * @param dataSchema input table schema.
	 * @param params     input parameters.
	 */
	public CalciteSelectMapper(TableSchema dataSchema, Params params) {
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
		addScalarFunctionConsumer.accept("REGEXP_REPLACE", StringFunctions.REGEXP_REPLACE);
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
	public void close() {
		super.close();
		try {
			for (PreparedStatement preparedStatement : threadPreparedStatementMap.values()) {
				preparedStatement.close();
			}
			for (Connection connection : threadConnectionMap.values()) {
				connection.close();
			}
		} catch (SQLException e) {
			throw new AkUnclassifiedErrorException("Failed to close prepared statement or connection.", e);
		}
	}

	private Connection getConnection() {
		/*
		In EAS, threads started have no contextClassLoader, which makes {@link
		CompilerFactoryFactory#getDefaultCompilerFactory} failed. So we manually set it to the classloader of this
		class.
		 */
		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
		try {
			Class.forName("org.apache.calcite.jdbc.Driver");
			return DriverManager.getConnection("jdbc:calcite:fun=mysql");
		} catch (ClassNotFoundException | SQLException e) {
			throw new AkUnclassifiedErrorException("Failed to initialize JDBC connection.", e);
		}
	}

	private PreparedStatement getPreparedStatement() {
		Connection connection = threadConnectionMap.computeIfAbsent(Thread.currentThread(), d -> getConnection());
		CalciteConnection calciteConnection;
		try {
			calciteConnection = connection.unwrap(CalciteConnection.class);
		} catch (SQLException e) {
			throw new AkIllegalStateException("Failed to unwrap CalciteConnection instance.", e);
		}

		SchemaPlus rootSchema = calciteConnection.getRootSchema();
		registerFlinkBuiltInFunctions(rootSchema);

		TableSchema dataSchema = getDataSchema();
		String clause = params.get(SelectParams.CLAUSE);

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
			return calciteConnection.prepareStatement(query);
		} catch (SQLException e) {
			throw new AkParseErrorException(String.format("Failed to prepare query statement: %s", query), e);
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		PreparedStatement preparedStatement = threadPreparedStatementMap.computeIfAbsent(
			Thread.currentThread(), d -> getPreparedStatement());

		for (int i = 0; i < selection.length(); i += 1) {
			Object v = selection.get(i);
			if (v instanceof BigDecimal) {
				preparedStatement.setObject(i + 1, v, java.sql.Types.DECIMAL);
			} else if (v instanceof BigInteger) {
				preparedStatement.setObject(i + 1, v, java.sql.Types.BIGINT);
			} else {
				preparedStatement.setObject(i + 1, v);
			}
		}
		try (ResultSet resultSet = preparedStatement.executeQuery()) {
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			if (resultSet.next()) {
				for (int i = 0; i < columnCount; i += 1) {
					result.set(i, resultSet.getObject(i + 1));
				}
			}
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		return prepareIoSchemaImpl(dataSchema, params);
	}

	static Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchemaImpl(TableSchema dataSchema,
																							Params params) {
		String clause = params.get(SelectParams.CLAUSE);
		MemSourceBatchOp source = new MemSourceBatchOp(Collections.emptyList(), dataSchema);
		TableSchema outputSchema = source.linkTo(new SelectBatchOp().setClause(clause)).getSchema();
		return Tuple4.of(
			dataSchema.getFieldNames(),
			outputSchema.getFieldNames(),
			outputSchema.getFieldTypes(),
			new String[0]
		);
	}
}
