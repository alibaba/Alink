package com.alibaba.alink.operator.common.statistics.basicstatistic;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * It is correlation result, which has colNames and correlation values.
 */
public class CorrelationResult implements Serializable {

	private static final long serialVersionUID = -498543486504426397L;
	/**
	 * correlation data.
	 */
	DenseMatrix correlation;

	/**
	 * If it is vector correlation, colNames is null.
	 */
	String[] colNames;

	public CorrelationResult(DenseMatrix correlation) {
		this.correlation = correlation;
	}

	public CorrelationResult(DenseMatrix correlation, String[] colNames) {
		this.correlation = correlation;
		this.colNames = colNames;
	}

	public double[][] getCorrelation() {
		return correlation.getArrayCopy2D();
	}

	public DenseMatrix getCorrelationMatrix() {
		return correlation;
	}

	public String[] getColNames() {
		return colNames;
	}

	@Override
	public String toString() {
		int n = correlation.numRows();

		Object[][] data = new Object[n][n];
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				data[i][j] = correlation.get(i, j);
			}
		}

		String[] outColNames = new String[n];
		if (colNames != null) {
			System.arraycopy(colNames, 0, outColNames, 0, n);
		} else {
			for (int i = 0; i < n; i++) {
				outColNames[i] = String.valueOf(i);
			}
		}

		return "Correlation:" +
			"\n" +
			PrettyDisplayUtils.displayTable(data, n, n, outColNames, outColNames, "colName", 20, 20);

	}
}
