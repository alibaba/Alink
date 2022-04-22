package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.probabilistic.IDF;
import com.alibaba.alink.params.outlier.CooksDistanceDetectorParams;
import com.alibaba.alink.params.outlier.WithMultiVarParams;

import java.util.HashMap;
import java.util.Map;

/**
 * Cook's distance is a linear model detector.
 * http://www.stat.ucla.edu/~nchristo/statistics100C/1268249.pdf
 */
public class CooksDistanceDetector extends OutlierDetector {

	public CooksDistanceDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	public Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast) throws Exception {
		MTable mt = getMTableWithLabel(series, this.params);

		int n = series.getNumRow();
		int p = series.getNumCol();

		if (n < p) {
			throw new RuntimeException("rowNum must be larger than colNum-1.");
		}

		DenseMatrix X = new DenseMatrix(n, p);
		DenseMatrix Y = new DenseMatrix(n, 1);

		for (int i = 0; i < n; i++) {
			Y.set(i, 0, getDoubleValue(mt, i, p - 1));
			for (int j = 0; j < p; j++) {
				if (p - 1 == j) {
					X.set(i, j, 1.0);
				} else {
					X.set(i, j, getDoubleValue(mt, i, j));
				}
			}
		}

		double[] d = cooksDistance(X, Y);

		double f = IDF.F(0.95, p, n - p);

		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[n];
		for (int i = 0; i < n; i++) {
			HashMap <String, String> infoMap = new HashMap <>();
			infoMap.put("distance", String.valueOf(d[i]));
			infoMap.put("n", String.valueOf(n));
			infoMap.put("p", String.valueOf(p));
			infoMap.put("f", String.valueOf(f));
			results[i] = Tuple3.of(d[i] > f, d[i], infoMap);
		}

		return results;
	}

	static MTable getMTableWithLabel(MTable series, Params params) {
		if (params.contains(WithMultiVarParams.VECTOR_COL)) {
			Tuple2 <Vector[], Integer> vectorsAndShape = OutlierUtil.selectVectorCol(
				series, params.get(WithMultiVarParams.VECTOR_COL)
			);
			return OutlierUtil.vectorsToMTable(vectorsAndShape.f0, vectorsAndShape.f1);
		} else {
			String[] featureCols = params.get(WithMultiVarParams.FEATURE_COLS);
			String labelCol = params.get(CooksDistanceDetectorParams.LABEL_COL);
			String[] mTableCols = series.getColNames();
			if (null != labelCol && !labelCol.trim().isEmpty()) {
				if (featureCols == null) {
					featureCols = new String[mTableCols.length];
					featureCols[featureCols.length - 1] = labelCol;
					int idx = 0;
					for (String col : mTableCols) {
						if (col.equals(labelCol)) {
							continue;
						}
						featureCols[idx++] = col;
					}
				} else {
					String[] newFeatureCols = new String[featureCols.length + 1];
					System.arraycopy(featureCols, 0, newFeatureCols, 0, featureCols.length);
					newFeatureCols[featureCols.length] = labelCol;
					featureCols = newFeatureCols;
				}
			}
			if (featureCols == null) {
				featureCols = mTableCols;
			}

			return OutlierUtil.selectFeatures(series, featureCols);
		}
	}

	static double[] cooksDistance(DenseMatrix X, DenseMatrix Y) {
		int n = X.numRows();
		int p = X.numCols();

		DenseMatrix xt = X.transpose();
		DenseMatrix xtx = xt.multiplies(X);
		DenseMatrix xtxInv = xtx.pseudoInverse();

		DenseMatrix beta = xtxInv.multiplies(xt.multiplies(Y));

		double[] v = new double[n];
		for (int i = 0; i < n; i++) {
			DenseMatrix xi = X.getSubMatrix(i, i + 1, 0, p);
			v[i] = xi.multiplies(xtxInv).multiplies(xi.transpose()).get(0, 0);
		}
		DenseMatrix residuals = Y.minus(X.multiplies(beta));

		double s2 = residuals.transpose().multiplies(residuals).get(0, 0) / (n - p);
		double[] d = new double[n];
		for (int i = 0; i < n; i++) {
			d[i] = Math.pow(residuals.get(i, 0), 2) * v[i] / s2 / Math.pow(1 - v[i], 2) / p;
		}
		return d;
	}

	private double getDoubleValue(MTable mTable, int iRow, int iCol) {
		Object obj = mTable.getEntry(iRow, iCol);
		if (null == obj) {
			throw new RuntimeException(String.format("the entry of %s row and %s col is null.", iRow, iCol));
		}
		return ((Number) mTable.getEntry(iRow, iCol)).doubleValue();
	}

}
