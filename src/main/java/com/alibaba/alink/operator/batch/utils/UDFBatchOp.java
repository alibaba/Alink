package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.ScalarFunction;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.UDFParams;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 * outputColNames CAN NOT have the same colName with reservedColName except the selectedColName.
 */
public class UDFBatchOp extends BatchOperator <UDFBatchOp>
	implements UDFParams <UDFBatchOp> {

	private ScalarFunction udf;

	public UDFBatchOp() {
		super(null);
	}

	public UDFBatchOp(Params params) {
		super(params);
	}

	public UDFBatchOp setFunc(ScalarFunction udf) {
		this.udf = udf;
		return this;
	}

	//public UDFBatchOp(String selectedColName, String outputColName, ScalarFunction udf) {
	//	this(selectedColName, new String[] {outputColName}, udf);
	//}
	//
	//public UDFBatchOp(String selectedColName, String outputColName, ScalarFunction udf, String[] reservedColNames) {
	//	this(selectedColName, new String[] {outputColName}, udf, reservedColNames);
	//}
	//
	//public UDFBatchOp(String selectedColName, String[] outputColNames, ScalarFunction udf) {
	//	this(selectedColName, outputColNames, udf, null);
	//}
	//
	//public UDFBatchOp(String selectedColName, String[] outputColNames, ScalarFunction udf, String[]
	//	reservedColNames) {
	//	super(null);
	//	if (null == this.udf) {
	//		throw new IllegalArgumentException("Must set the ScalarFunction!");
	//	} else {
	//		this.selectedColName = selectedColName;
	//		if (null == outputColNames || (outputColNames.length == 1 && outputColNames[0] == null)) {
	//			this.outputColNames = new String[] {this.selectedColName};
	//		} else {
	//			this.outputColNames = outputColNames;
	//		}
	//		this.udf = udf;
	//		this.reservedColNames = reservedColNames;
	//	}
	//}

	@Override
	public UDFBatchOp linkFrom(BatchOperator <?>... inputs) {
		if (null == this.udf) {
			throw new IllegalArgumentException("Must set the ScalarFunction!");
		}
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String[] inColNames = in.getColNames();
		String[] outputColNames = new String[] {getOutputCol()};
		String selectedColName = getSelectedCols()[0];
		String[] reservedColNames = getReservedCols();

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
			this.setOutputTable(exec(in, selectedColName, outputColNames[0], (ScalarFunction) udf,
				reservedColNames, MLEnvironmentFactory.get(getMLEnvironmentId())));
		} else {
			// selectedColName is in the outputColNames, then it can not in the reservedColNames
			String clauseAS = StringUtils.join(ArrayUtils.addAll(reservedColNames, outputColNames), ",");
			String tempColName = selectedColName + "_ml" + Long.toString(System.currentTimeMillis());
			int idx = TableUtil.findColIndex(outputColNames, selectedColName);
			outputColNames[idx] = tempColName;
			this.setOutputTable(exec(in, selectedColName, outputColNames[0], (ScalarFunction) udf,
				reservedColNames, MLEnvironmentFactory.get(getMLEnvironmentId())).as(clauseAS));
		}

		return this;
	}

	private static Table exec(BatchOperator in, String selectColName, String newColName, ScalarFunction sf,
							  String[] keepOldColNames, MLEnvironment session) {
		String fname = "f" + Long.toString(System.currentTimeMillis());
		session.getBatchTableEnvironment().registerFunction(fname, sf);
		String[] colNames = keepOldColNames;
		if (null == colNames) {
			colNames = in.getColNames();
		}
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < colNames.length; i++) {
			sbd.append(colNames[i]).append(", ");
		}
		sbd.append(fname).append("(").append(selectColName).append(") as ").append(newColName);

		return session.getBatchTableEnvironment().sqlQuery("SELECT " + sbd.toString() + " FROM " + in.getOutputTable());
	}

}
