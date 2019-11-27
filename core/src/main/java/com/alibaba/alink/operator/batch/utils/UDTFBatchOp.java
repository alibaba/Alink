package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.UDTFParams;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 * outputColNames CAN NOT have the same colName with reservedColName except the selectedColName.
 */
public class UDTFBatchOp extends BatchOperator <UDTFBatchOp>
	implements UDTFParams <UDTFBatchOp> {

	private TableFunction udf;

	public UDTFBatchOp() {
		super(null);
	}

	public UDTFBatchOp(Params params) {
		super(params);
	}

	public UDTFBatchOp setFunc(TableFunction udf) {
		this.udf = udf;
		return this;
	}

	//public UDTFBatchOp(String selectedColName, String outputColName, TableFunction udf) {
	//	this(selectedColName, new String[] {outputColName}, udf);
	//}
	//
	//public UDTFBatchOp(String selectedColName, String outputColName, TableFunction udf, String[] reservedColNames) {
	//	this(selectedColName, new String[] {outputColName}, udf, reservedColNames);
	//}
	//
	//public UDTFBatchOp(String selectedColName, String[] outputColNames, TableFunction udf) {
	//	this(selectedColName, outputColNames, udf, null);
	//}
	//
	//public UDTFBatchOp(String selectedColName, String[] outputColNames, TableFunction udf, String[]
	//	reservedColNames) {
	//	super(null);
	//	if (null == selectedColName) {
	//		throw new RuntimeException("Must input selectedColName!");
	//	} else {
	//		selectedColName = selectedColName;
	//		if (null == outputColNames || (outputColNames.length == 1 && outputColNames[0] == null)) {
	//			outputColNames = new String[] {selectedColName};
	//		} else {
	//			outputColNames = outputColNames;
	//		}
	//		this.udf = udf;
	//		reservedColNames = reservedColNames;
	//	}
	//}

	@Override
	public UDTFBatchOp linkFrom(BatchOperator <?>... inputs) {
		if (null == this.udf) {
			throw new IllegalArgumentException("Must set the TableFunction!");
		}
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String[] outputColNames = getOutputCols();
		String selectedColName = getSelectedCols()[0];
		String[] reservedColNames = getReservedCols();

		String[] inColNames = in.getColNames();
		int inputColIndex = TableUtil.findColIndex(inColNames, selectedColName);
		if (inputColIndex < 0) {
			throw new RuntimeException("Input data table NOT have the col:" + selectedColName);
		}

		if (null == reservedColNames) {
			if (TableUtil.findColIndex(outputColNames, selectedColName) >= 0) {
				reservedColNames = new String[inColNames.length - 1];
				for (int i = 0; i < inputColIndex; i++) {
					reservedColNames[i] = inColNames[i];
				}
				for (int i = inputColIndex + 1; i < inColNames.length; i++) {
					reservedColNames[i - 1] = inColNames[i];
				}
			} else {
				reservedColNames = in.getColNames();
			}
		}

		for (int k = 0; k < outputColNames.length; k++) {
			String[] reservedColNamesOld = reservedColNames;
			int outputColIndex = TableUtil.findColIndex(reservedColNamesOld, outputColNames[k]);
			if (outputColIndex >= 0) {
				reservedColNames = new String[reservedColNamesOld.length - 1];
				for (int i = 0; i < outputColIndex; i++) {
					reservedColNames[i] = reservedColNamesOld[i];
				}
				for (int i = outputColIndex + 1; i < reservedColNamesOld.length; i++) {
					reservedColNames[i - 1] = reservedColNamesOld[i];
				}
			}
		}

		boolean hasSameColName = false;
		for (String name : outputColNames) {
			if (TableUtil.findColIndex(reservedColNames, name) >= 0) {
				hasSameColName = true;
				break;
			}
		}
		if (hasSameColName) {
			throw new RuntimeException("reservedColNames has the same name with outputColNames.");
		}

		if (TableUtil.findColIndex(outputColNames, selectedColName) < 0) {
			// selectedColName NOT in the outputColNames
			this.setOutputTable(exec(in, selectedColName, outputColNames, (TableFunction <Row>) udf,
				reservedColNames, MLEnvironmentFactory.get(getMLEnvironmentId())));
		} else {
			// selectedColName is in the outputColNames, then it can not in the reservedColNames
			String clauseAS = StringUtils.join(ArrayUtils.addAll(reservedColNames, outputColNames), ",");
			String tempColName = selectedColName + "_ml" + Long.toString(System.currentTimeMillis());
			int idx = TableUtil.findColIndex(outputColNames, selectedColName);
			outputColNames[idx] = tempColName;
			this.setOutputTable(exec(in, selectedColName, outputColNames, (TableFunction <Row>) udf,
				reservedColNames, MLEnvironmentFactory.get(getMLEnvironmentId())).as(clauseAS));
		}

		return this;
	}

	private static String MappingColNameIgnoreCase(String[] allColNames, String colName) throws Exception {
		for (String allColName : allColNames) {
			if (allColName.equalsIgnoreCase(colName)) {
				return allColName;
			}
		}

		throw new Exception("Can not find " + colName + " in op");
	}

	private static Table exec(BatchOperator in, String selectColName, String[] newColNames, TableFunction <Row> tf,
							  String[] keepOldColNames, MLEnvironment session) {
		String fname = "f" + Long.toString(System.currentTimeMillis());
		session.getBatchTableEnvironment().registerFunction(fname, tf);
		String[] colNames = keepOldColNames;
		if (null == colNames) {
			colNames = in.getColNames();
		}

		try {
			selectColName = MappingColNameIgnoreCase(in.getColNames(), selectColName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		StringBuilder sbd;
		sbd = new StringBuilder();
		sbd.append(", LATERAL TABLE(").append(fname).append("(").append(selectColName).append(")) as T(").append(
			newColNames[0]);
		for (int i = 1; i < newColNames.length; i++) {
			sbd.append(", ").append(newColNames[i]);
		}
		sbd.append(")");
		String joinClause = sbd.toString();

		String selectClause = StringUtils.join(ArrayUtils.addAll(colNames, newColNames), ",");

		String sqlClause = "SELECT " + selectClause + " FROM " + in.getOutputTable() + joinClause;

		return session.getBatchTableEnvironment().sqlQuery(sqlClause);
	}

}
