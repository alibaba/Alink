package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.SingleInputUdfOperator;
import org.apache.flink.api.java.operators.TwoInputUdfOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;

/**
 * Provide functions of conversions between DataSet and Table.
 */
public class DataSetConversionUtil {
	/**
	 * Convert the given Table to {@link DataSet}<{@link Row}>.
	 *
	 * @param sessionId the sessionId of {@link MLEnvironmentFactory}
	 * @param table     the Table to convert.
	 * @return the converted DataSet.
	 */
	public static DataSet <Row> fromTable(Long sessionId, Table table) {
		return MLEnvironmentFactory
			.get(sessionId)
			.getBatchTableEnvironment()
			.toDataSet(table, Row.class);
	}

	/**
	 * Convert the given DataSet into a Table with specified TableSchema.
	 *
	 * @param sessionId the sessionId of {@link MLEnvironmentFactory}
	 * @param data      the DataSet to convert.
	 * @param schema    the specified TableSchema.
	 * @return the converted Table.
	 */
	public static Table toTable(Long sessionId, DataSet <Row> data, TableSchema schema) {
		return toTable(sessionId, data, schema.getFieldNames(), schema.getFieldTypes());
	}

	/**
	 * Convert the given DataSet into a Table with specified colNames and colTypes.
	 *
	 * @param sessionId sessionId the sessionId of {@link MLEnvironmentFactory}.
	 * @param data      the DataSet to convert.
	 * @param colNames  the specified colNames.
	 * @param colTypes  the specified colTypes. This variable is used only when the
	 *                  DataSet is produced by a function and Flink cannot determine
	 *                  automatically what the produced type is.
	 * @return the converted Table.
	 */
	public static Table toTable(Long sessionId, DataSet <Row> data, String[] colNames, TypeInformation <?>[]
		colTypes) {
		return toTable(MLEnvironmentFactory.get(sessionId), data, colNames, colTypes);
	}

	/**
	 * Convert the given DataSet into a Table with specified colNames.
	 *
	 * @param sessionId sessionId the sessionId of {@link MLEnvironmentFactory}.
	 * @param data      the DataSet to convert.
	 * @param colNames  the specified colNames.
	 * @return the converted Table.
	 */
	public static Table toTable(Long sessionId, DataSet <Row> data, String[] colNames) {
		return toTable(MLEnvironmentFactory.get(sessionId), data, colNames);
	}

	/**
	 * Convert the given DataSet into a Table with specified colNames and colTypes.
	 *
	 * @param session  the MLEnvironment using to convert DataSet to Table.
	 * @param data     the DataSet to convert.
	 * @param colNames the specified colNames.
	 * @param colTypes the specified colTypes. This variable is used only when the
	 *                 DataSet is produced by a function and Flink cannot determine
	 *                 automatically what the produced type is.
	 * @return the converted Table.
	 */
	public static Table toTable(MLEnvironment session, DataSet <Row> data, String[] colNames, TypeInformation <?>[]
		colTypes) {
		try {
			// Try to add row type information for the dataset to be converted.
			// In most case, this keeps us from the rolling back logic in the catch block,
			// which adds an unnecessary map function just in order to add row type information.
			try {
				if (data instanceof SingleInputUdfOperator) {
					((SingleInputUdfOperator) data).returns(new RowTypeInfo(colTypes, colNames));
				} else if (data instanceof TwoInputUdfOperator) {
					((TwoInputUdfOperator) data).returns(new RowTypeInfo(colTypes, colNames));
				}
			} catch (IllegalStateException ex) {
				//pass
			}

			return toTable(session, data, colNames);
		} catch (ValidationException ex) {
			if (null == colTypes) {
				throw ex;
			} else {
				DataSet <Row> t = getDataSetWithExplicitTypeDefine(data, colNames, colTypes);
				return toTable(session, t, colNames);
			}
		}
	}

	/**
	 * Convert the given DataSet into a Table with specified colNames.
	 *
	 * @param session  the MLEnvironment using to convert DataSet to Table.
	 * @param data     the DataSet to convert.
	 * @param colNames the specified colNames.
	 * @return the converted Table.
	 */
	public static Table toTable(MLEnvironment session, DataSet <Row> data, String[] colNames) {
		if (null == colNames || colNames.length == 0) {
			return session.getBatchTableEnvironment().fromDataSet(data);
		} else {
			StringBuilder sbd = new StringBuilder();
			sbd.append(colNames[0]);
			for (int i = 1; i < colNames.length; i++) {
				sbd.append(",").append(colNames[i]);
			}
			return session.getBatchTableEnvironment().fromDataSet(data, sbd.toString());
		}
	}

	/**
	 * Adds a type information hint about the colTypes with the Row to the DataSet.
	 *
	 * @param data     the DataSet to add type information.
	 * @param colNames the specified colNames
	 * @param colTypes the specified colTypes
	 * @return the DataSet with type information hint.
	 */
	private static DataSet <Row> getDataSetWithExplicitTypeDefine(
		DataSet <Row> data,
		String[] colNames,
		TypeInformation <?>[] colTypes) {

		DataSet <Row> r = data
			.map(
				new MapFunction <Row, Row>() {
					private static final long serialVersionUID = 4962235907261477641L;

					@Override
					public Row map(Row t) throws Exception {
						return t;
					}
				}
			)
			.returns(new RowTypeInfo(colTypes, colNames));

		return r;
	}

}
