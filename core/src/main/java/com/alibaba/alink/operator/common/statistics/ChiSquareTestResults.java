package com.alibaba.alink.operator.common.statistics;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;

/**
 * chi-square test result.
 */
public class ChiSquareTestResults implements Serializable {
	private static final long serialVersionUID = 4607796796072965555L;
	public ChiSquareTestResult[] results;
	public String[] selectedCols;

	@Override
	public String toString() {
		Object[][] data = new Object[results.length][3];
		for (int i = 0; i < results.length; i++) {
			data[i][0] = results[i].getP();
			data[i][1] = results[i].getValue();
			data[i][2] = results[i].getDf();
		}

		return "ChiSquareTest:"
			+ "\n"
			+ PrettyDisplayUtils.displayTable(data, results.length, 3,
			selectedCols, new String[] {"p", "value", "df"}, "col");
	}

	public ChiSquareTestResult[] getResults() {
		return results;
	}

	public String[] getSelectedCols() {
		return selectedCols;
	}

	public ChiSquareTestResult getResult(String colName) {
		return results[TableUtil.findColIndexWithAssert(selectedCols, colName)];
	}
}