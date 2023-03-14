package com.alibaba.alink.operator.common.statistics;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.jama.JMatrixFunc;
import com.alibaba.alink.common.utils.TableUtil;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class CorrespondenceAnalysis {

	public static CorrespondenceAnalysisResult calc(Iterable <Row> row, String rowName, String colName,
													String[] colNames) throws Exception {
		int rowIdx = TableUtil.findColIndexWithAssertAndHint(colNames, rowName);
		int colIdx = TableUtil.findColIndexWithAssertAndHint(colNames, colName);

		Map <GroupFactor, Long> groupCount = getGroupCount(row, rowIdx, colIdx);
		List <Set <String>> distinctValue = getDistinctValue(groupCount);
		String[] rowTags = distinctValue.get(0).toArray(new String[0]);
		String[] colTags = distinctValue.get(1).toArray(new String[1]);
		double[][] tabs = getPivotTable(rowTags, colTags, groupCount);

		CorrespondenceAnalysisResult car = calc(tabs);
		car.rowLegend = rowName;
		car.colLegend = colName;
		car.rowTags = rowTags;
		car.colTags = colTags;
		return car;
	}

	private static Map <GroupFactor, Long> getGroupCount(Iterable <Row> row, int rowIdx, int colIdx) {
		Map <GroupFactor, Long> groupCount = new TreeMap <GroupFactor, Long>(
			new Comparator <GroupFactor>() {
				public int compare(GroupFactor left, GroupFactor right) {
					int r = left.rowExpr.compareTo(right.rowExpr);
					if (r == 0) {
						return left.colExpr.compareTo(right.colExpr);
					}
					return r;
				}
			});

		for (Row objs : row) {
			GroupFactor factor = new GroupFactor(objs.getField(rowIdx).toString(), objs.getField(colIdx).toString());
			if (groupCount.containsKey(factor)) {
				groupCount.put(factor, groupCount.get(factor) + 1);
			} else {
				groupCount.put(factor, 1L);
			}
		}

		return groupCount;
	}

	private static List <Set <String>> getDistinctValue(Map <GroupFactor, Long> groupCount) {
		Set <String> rowExprs = new HashSet <String>();
		Set <String> colExprs = new HashSet <String>();
		Set <GroupFactor> groupFactors = groupCount.keySet();
		for (GroupFactor i : groupFactors) {
			rowExprs.add(i.rowExpr);
			colExprs.add(i.colExpr);
		}
		List <Set <String>> Exprs = new ArrayList <Set <String>>();
		Exprs.add(rowExprs);
		Exprs.add(colExprs);
		return Exprs;
	}

	static double[][] getPivotTable(String[] rowTags,
									String[] colTags,
									Map <GroupFactor, Long> groupCount) {
		double[][] tabs = new double[rowTags.length][colTags.length];
		for (int i = 0; i < rowTags.length; i++) {
			for (int j = 0; j < colTags.length; j++) {
				GroupFactor factor = new GroupFactor(rowTags[i], colTags[j]);
				if (groupCount.containsKey(factor)) {
					tabs[i][j] = groupCount.get(factor);
				} else {
					tabs[i][j] = 0;
				}
			}
		}
		return tabs;
	}

	static CorrespondenceAnalysisResult calc(double[][] X) throws Exception {
		int nrow = X.length;
		int ncol = X[0].length;
		if (nrow * ncol == 1) {
			throw new Exception("(the number of column expr) * ( number of row expr) must Greater than 2.!");
		}

		double T = 0.0;
		for (double[] aX : X) {
			for (int j = 0; j < ncol; j++) {
				T += aX[j];
			}
		}

		double[] wrow = new double[nrow];
		double[] wcol = new double[ncol];

		for (int i = 0; i < nrow; i++) {
			double s = 0;
			for (int j = 0; j < ncol; j++) {
				s += X[i][j];
			}
			wrow[i] = s;
		}
		for (int j = 0; j < ncol; j++) {
			double s = 0;
			for (double[] aX : X) {
				s += aX[j];
			}
			wcol[j] = s;
		}

		double[][] P = new double[nrow][ncol];
		for (int i = 0; i < nrow; i++) {
			for (int j = 0; j < ncol; j++) {
				P[i][j] = X[i][j] / T;
			}
		}

		double[] Pr = new double[nrow];
		double[] Pc = new double[ncol];
		for (int i = 0; i < nrow; i++) {
			double s = 0;
			for (int j = 0; j < ncol; j++) {
				s += P[i][j];
			}
			Pr[i] = s;
		}
		for (int j = 0; j < ncol; j++) {
			double s = 0;
			for (int i = 0; i < nrow; i++) {
				s += P[i][j];
			}
			Pc[j] = s;
		}

		double[][] Z = new double[nrow][ncol];
		for (int i = 0; i < nrow; i++) {
			for (int j = 0; j < ncol; j++) {
				double t = Pr[i] * Pc[j];
				Z[i][j] = (P[i][j] - t) / Math.sqrt(t);
			}
		}

		double chi2 = 0;
		for (int i = 0; i < nrow; i++) {
			for (int j = 0; j < ncol; j++) {
				chi2 += Z[i][j] * Z[i][j];
			}
		}
		chi2 *= T;

		DenseMatrix[] ed = JMatrixFunc.svd(new DenseMatrix(Z));

		int p = Math.min(nrow, ncol);

		DenseMatrix Mr = ed[0].multiplies(ed[1]);
		for (int i = 0; i < nrow; i++) {
			double t = Math.sqrt(Pr[i]);
			for (int j = 0; j < p; j++) {
				Mr.set(i, j, Mr.get(i, j) / t);
			}
		}

		DenseMatrix Mc = ed[2].multiplies(ed[1]);
		for (int i = 0; i < ncol; i++) {
			double t = Math.sqrt(Pc[i]);
			for (int j = 0; j < p; j++) {
				Mc.set(i, j, Mc.get(i, j) / t);
			}
		}

		CorrespondenceAnalysisResult ca = new CorrespondenceAnalysisResult();
		ca.nrow = nrow;
		ca.ncol = ncol;
		ca.rowPos = new double[nrow][2];
		if (Mr.numCols() > 1) {
			for (int i = 0; i < nrow; i++) {
				ca.rowPos[i][0] = Mr.get(i, 0);
				ca.rowPos[i][1] = Mr.get(i, 1);
			}
		} else {
			for (int i = 0; i < nrow; i++) {
				ca.rowPos[i][0] = Mr.get(i, 0);
				ca.rowPos[i][1] = 0;
			}
		}
		ca.colPos = new double[ncol][2];
		if (Mr.numCols() > 1) {
			for (int i = 0; i < ncol; i++) {
				ca.colPos[i][0] = Mc.get(i, 0);
				ca.colPos[i][1] = Mc.get(i, 1);
			}
		} else {
			for (int i = 0; i < ncol; i++) {
				ca.colPos[i][0] = Mc.get(i, 0);
				ca.colPos[i][1] = 0;
			}
		}
		ca.sv = new double[2];
		ca.sv[0] = ed[1].get(0, 0);
		if (Mr.numCols() > 1) {
			ca.sv[1] = ed[1].get(1, 1);
		} else {
			ca.sv[1] = 0;
		}
		ca.pct = new double[2];
		ca.pct[0] = T * ca.sv[0] * ca.sv[0] / chi2;
		ca.pct[1] = T * ca.sv[1] * ca.sv[1] / chi2;

		return ca;
	}

	private static class GroupFactor {
		String rowExpr;
		String colExpr;

		public GroupFactor(String rowExpr, String colExpr) {
			this.rowExpr = rowExpr;
			this.colExpr = colExpr;
		}
	}
}
