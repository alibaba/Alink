package com.alibaba.alink.operator.stream.utils;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.dataproc.UDFParams;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * <p>
 * outputColNames CAN NOT have the same colName with keepOriginColName except the selectedColName.
 */
public class UDFStreamOp extends StreamOperator<UDFStreamOp>
	implements UDFParams<UDFStreamOp> {
	private ScalarFunction udf;

	public UDFStreamOp() {
		super(null);
	}

	public UDFStreamOp(Params params) {
		super(params);
	}

	public UDFStreamOp setFunc(ScalarFunction udf) {
		this.udf = udf;
		return this;
	}
	//private final UserDefinedFunction udf;
	//private final String[] outputColNames;
	//private String selectedColName;
	//private String[] reservedColNames;
	//
	//public UDFStreamOp(String selectedColName, String outputColName, UserDefinedFunction udf) {
	//	this(selectedColName, new String[] {outputColName}, udf);
	//}
	//
	//public UDFStreamOp(String selectedColName, String[] outputColNames, UserDefinedFunction udf) {
	//	this(selectedColName, outputColNames, udf, null);
	//}
	//
	//public UDFStreamOp(String selectedColName, String outputColName, UserDefinedFunction udf, String[] reservedColNames) {
	//	this(selectedColName, new String[] {outputColName}, udf, reservedColNames);
	//}
	//
	//public UDFStreamOp(String selectedColName, String[] outputColNames, UserDefinedFunction udf,
	//				   String[] reservedColNames) {
	//	super(null);
	//	if (null == selectedColName) {
	//		throw new RuntimeException("Must input selectedColName!");
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
	public UDFStreamOp linkFrom(StreamOperator<?>... inputs) {
		if (null == this.udf) {
			throw new IllegalArgumentException("Must set the ScalarFunction!");
		}
		StreamOperator<?> in = checkAndGetFirst(inputs);
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
				System.arraycopy(inColNames, 0, reservedColNames, 0, inputColIndex);
				System.arraycopy(inColNames, inputColIndex + 1, reservedColNames, inputColIndex + 1 - 1, inColNames.length - (inputColIndex + 1));
			} else {
				reservedColNames = in.getColNames();
			}
		}

		for (String outputColName : outputColNames) {
			String[] reservedColNamesOld = reservedColNames;
			int outputColIndex = TableUtil.findColIndex(reservedColNamesOld, outputColName);
			if (outputColIndex >= 0) {
				reservedColNames = new String[inColNames.length - 1];
				System.arraycopy(reservedColNamesOld, 0, reservedColNames, 0, outputColIndex);
				System.arraycopy(reservedColNamesOld, outputColIndex + 1, reservedColNames, outputColIndex + 1 - 1, inColNames.length - (outputColIndex + 1));
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
				this.setOutputTable(exec(in, selectedColName, outputColNames[0], udf,
					reservedColNames));
		} else {
			// selectedColName is in the outputColNames, then it can not in the reservedColNames
			String clauseAS = StringUtils.join(ArrayUtils.addAll(reservedColNames, outputColNames), ",");
			String tempColName = selectedColName + "_ml" + Long.toString(System.currentTimeMillis());
			int idx = TableUtil.findColIndex(outputColNames, selectedColName);
			outputColNames[idx] = tempColName;
				this.setOutputTable(exec(in, selectedColName, outputColNames[0], udf,
					reservedColNames).as(clauseAS));
		}

		return this;
	}

	private static Table exec(StreamOperator in, String selectColName, String newColName, ScalarFunction sf,
					  String[] keepOldColNames) {
		String fname = "f" + Long.toString(System.currentTimeMillis());

		MLEnvironmentFactory.get(in.getMLEnvironmentId()).getStreamTableEnvironment().registerFunction(fname, sf);
		String[] colNames = keepOldColNames;
		if (null == colNames) {
			colNames = in.getColNames();
		}
		StringBuilder sbd = new StringBuilder();
		for (String colName : colNames) {
			sbd.append(colName).append(", ");
		}
		sbd.append(fname).append("(").append(selectColName).append(") as ").append(newColName);

		return MLEnvironmentFactory.get(in.getMLEnvironmentId()).getStreamTableEnvironment().sqlQuery("SELECT " + sbd.toString() + " FROM " + in.getOutputTable());
	}

}
