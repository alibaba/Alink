package com.alibaba.alink.operator.common.statistics.basicstatistic;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;

import java.util.Arrays;

/**
 * It is summary for table， it will calculate statistics and return TableSummary.
 * You can get statistics from Summary.
 */
public class TableSummarizer extends BaseSummarizer {

	private static final long serialVersionUID = 4588962274305185787L;
	/**
	 * col names which will calculate.
	 */
	public String[] colNames;

	/**
	 * the value of ith row and jth col is sum of the ith variance
	 * when the ith col and the jth col of row are both not null.
	 * xSum_i_j = sum(x_i) when x_i != null && x_j!=null.
	 */
	DenseMatrix xSum;

	/**
	 * the value of ith row and jth col is sum of the ith variance
	 * when the ith col and the jth col of row are both not null.
	 * xSum_i_j = sum(x_i) when x_i != null && x_j!=null.
	 */
	DenseMatrix xSquareSum;

	/**
	 * the value of ith row and jth col is the count of the ith variance is not null
	 * and the jth variance is not null at the same row.
	 */
	DenseMatrix xyCount;

	/**
	 * numerical col indices:
	 * if col is numerical, it will calculate all statistics, otherwise only count, numMissingValue.
	 */
	private int[] numericalColIndices;

	/**
	 * the number of missing value of all columns.
	 */
	private DenseVector numMissingValue;

	/**
	 * sum_i = sum(x_i) when x_i is not null.
	 */
	protected DenseVector sum;

	/**
	 * squareSum_i = sum(x_i * x_i) when x_i is not null.
	 */
	protected DenseVector squareSum;

	/**
	 * sum3_i = sum(x_i * x_i * x_i) when x_i is not null.
	 */
	protected DenseVector sum3;

	/**
	 * min_i = min(x_i) when x_i is not null.
	 */
	protected DenseVector min;

	/**
	 * max_i = max(x_i) when x_i is not null.
	 */
	protected DenseVector max;

	/**
	 * normL1_i = normL1(x_i) = sum(|x_i|) when x_i is not null.
	 */
	protected DenseVector normL1;

	/**
	 * Intermediate variable which will used in Visit function.
	 */
	private Double[] vals;

	/**
	 * default constructor.
	 */
	private TableSummarizer() {
	}

	/**
	 * if col is numerical, it will calculate all statistics, otherwise only calculate count and numMissingValue.
	 * if calculateOuterProduct is false, outerProduct，xSum, xSquareSum, xyCount are not be used,
	 * these are for correlation and covariance.
	 */
	public TableSummarizer(String[] selectedColNames, int[] numericalColIndices, boolean calculateOuterProduct) {
		this.colNames = selectedColNames;
		this.calculateOuterProduct = calculateOuterProduct;
		this.numericalColIndices = numericalColIndices;
	}

	/**
	 * given row, incremental calculate statistics.
	 */
	public BaseSummarizer visit(Row row) {
		int n = row.getArity();
		int numberN = numericalColIndices.length;

		if (count == 0) {
			init();
		}

		count++;

		for (int i = 0; i < n; i++) {
			Object obj = row.getField(i);
			if (obj == null) {
				numMissingValue.add(i, 1);
			}
		}

		for (int i = 0; i < numberN; i++) {
			Object obj = row.getField(numericalColIndices[i]);
			if (obj != null) {
				if (obj instanceof Boolean) {
					vals[i] = (boolean) obj ? 1.0 : 0.0;
				} else {
					vals[i] = ((Number) obj).doubleValue();
				}
			} else {
				vals[i] = null;
			}
		}
		for (int i = 0; i < numberN; i++) {
			if (vals[i] != null) {
				double val = vals[i];

				max.set(i, Math.max(val, max.get(i)));
				min.set(i, Math.min(val, min.get(i)));

				sum.add(i, val);
				squareSum.add(i, val * val);
				sum3.add(i, val * val * val);

				normL1.add(i, Math.abs(val));

				if (calculateOuterProduct) {
					for (int j = i; j < numberN; j++) {
						if (vals[j] != null) {
							outerProduct.add(i, j, val * vals[j]);
							xSum.add(i, j, val);
							xSquareSum.add(i, j, val * val);
							xyCount.add(i, j, 1);
							if (j != i) {
								xSum.add(j, i, vals[j]);
								xSquareSum.add(j, i, vals[j] * vals[j]);
								xyCount.add(j, i, 1);
							}
						}
					}
				}
			}
		}
		return this;
	}

	/**
	 * n is the number of columns participating in the calculation.
	 */
	private void init() {
		int n = colNames.length;
		int numberN = numericalColIndices.length;

		numMissingValue = new DenseVector(n);
		sum = new DenseVector(numberN);
		squareSum = new DenseVector(numberN);
		sum3 = new DenseVector(numberN);
		normL1 = new DenseVector(numberN);

		double[] minVals = new double[numberN];
		Arrays.fill(minVals, Double.MAX_VALUE);
		min = new DenseVector(minVals);

		double[] maxVals = new double[numberN];
		Arrays.fill(maxVals, -Double.MAX_VALUE);
		max = new DenseVector(maxVals);

		if (calculateOuterProduct) {
			outerProduct = new DenseMatrix(numberN, numberN);
			xSum = new DenseMatrix(numberN, numberN);
			xSquareSum = new DenseMatrix(numberN, numberN);
			xyCount = new DenseMatrix(numberN, numberN);
		}

		vals = new Double[numberN];
	}

	/**
	 * merge left and right, return  a new summary. left will be changed.
	 */
	public static TableSummarizer merge(TableSummarizer left, TableSummarizer right) {
		if (right.count == 0) {
			return left;
		}

		if (left.count == 0) {
			return right.copy();
		}

		left.count += right.count;
		left.numMissingValue.plusEqual(right.numMissingValue);
		left.sum.plusEqual(right.sum);
		left.squareSum.plusEqual(right.squareSum);
		left.sum3.plusEqual(right.sum3);
		left.normL1.plusEqual(right.normL1);
		MatVecOp.apply(left.min, right.min, left.min, Math::min);
		MatVecOp.apply(left.max, right.max, left.max, Math::max);

		if (left.outerProduct != null && right.outerProduct != null) {
			left.outerProduct.plusEquals(right.outerProduct);
			left.xSum.plusEquals(right.xSum);
			left.xSquareSum.plusEquals(right.xSquareSum);
			left.xyCount.plusEquals(right.xyCount);
		} else if (left.outerProduct == null && right.outerProduct != null) {
			left.outerProduct = right.outerProduct.clone();
			left.xSum = right.xSum.clone();
			left.xSquareSum = right.xSquareSum.clone();
			left.xyCount = right.xyCount.clone();
		}

		return left;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder()
			.append("count: ")
			.append(count)
			.append("\n");
		if (count != 0) {
			sbd.append("sum: ")
				.append(VectorUtil.toString(sum))
				.append("\n")
				.append("squareSum: ")
				.append(VectorUtil.toString(squareSum))
				.append("\n")
				.append("min: ")
				.append(VectorUtil.toString(min))
				.append("\n")
				.append("max: ")
				.append(VectorUtil.toString(max));
		}

		return sbd.toString();
	}

	/**
	 * get summary result, you can get statistics from summary.
	 */
	public TableSummary toSummary() {
		TableSummary summary = new TableSummary();

		summary.count = count;
		summary.sum = sum;
		summary.squareSum = squareSum;
		summary.sum3 = sum3;
		summary.normL1 = normL1;
		summary.min = min;
		summary.max = max;

		summary.numMissingValue = numMissingValue;
		summary.numericalColIndices = numericalColIndices;

		summary.colNames = colNames;

		return summary;
	}

	/**
	 * get summary result of selected columns.
	 */
	public TableSummary toSummary(String[] selectedColNames) {
		if (selectedColNames.length == 0) {
			return toSummary();
		}
		TableSummary summary = new TableSummary();
		int[] selectedColIndex = TableUtil.findColIndices(colNames, selectedColNames);
		int n = selectedColNames.length;

		summary.count = count;
		summary.sum = new DenseVector(n);
		summary.squareSum = new DenseVector(n);
		summary.sum3 = new DenseVector(n);
		summary.normL1 = new DenseVector(n);
		summary.min = new DenseVector(n);
		summary.max = new DenseVector(n);
		summary.numMissingValue = new DenseVector(n);
		summary.numericalColIndices = new int[n];

		for (int i = 0; i < selectedColIndex.length; i++) {
			int targetIndex = selectedColIndex[i];
			int targetIndexInDenseVector = -1;

			for (int j = 0; j < numericalColIndices.length; j++) {
				if (targetIndex == numericalColIndices[j]) {
					targetIndexInDenseVector = j;
				}
			}
			if (targetIndexInDenseVector == -1) {
				summary.numericalColIndices[i] = -1;
				continue;
			}

			summary.sum.set(i, sum.get(targetIndexInDenseVector));
			summary.squareSum.set(i, squareSum.get(targetIndexInDenseVector));
			summary.sum3.set(i, sum3.get(targetIndexInDenseVector));
			summary.normL1.set(i, normL1.get(targetIndexInDenseVector));
			summary.min.set(i, min.get(targetIndexInDenseVector));
			summary.max.set(i, max.get(targetIndexInDenseVector));

			summary.numMissingValue.set(i, numMissingValue.get(targetIndexInDenseVector));
			summary.numericalColIndices[i] = TableUtil.findColIndex(selectedColNames,
				colNames[numericalColIndices[targetIndexInDenseVector]]);
		}

		summary.colNames = selectedColNames;

		return summary;
	}

	/**
	 * when calculate correlation(x,y), if x or y is null, it will not involved in calculate.
	 */
	@Override
	public CorrelationResult correlation() {
		if (outerProduct == null) {
			return null;
		}

		DenseMatrix cov = covariance();
		int n = cov.numRows();

		for (int i = 0; i < numericalColIndices.length; i++) {
			int idxI = numericalColIndices[i];
			for (int j = 0; j < numericalColIndices.length; j++) {
				int idxJ = numericalColIndices[j];
				double val = cov.get(idxI, idxJ);
				if (!Double.isNaN(val)) {
					if (val != 0) {
						//it is not equal with variance(i).
						double varianceI = Math.max(0.0,
							(xSquareSum.get(i, j) - xSum.get(i, j) * xSum.get(i, j)
								/ xyCount.get(i, j)) / (xyCount.get(i, j) - 1));
						double varianceJ = Math.max(0.0,
							(xSquareSum.get(j, i) - xSum.get(j, i) * xSum.get(j, i)
								/ xyCount.get(j, i)) / (xyCount.get(j, i) - 1));

						double d = val / Math.sqrt(varianceI * varianceJ);
						cov.set(idxI, idxJ, d);
					}
				}
			}
		}

		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				if (!Double.isNaN(cov.get(i, j))) {
					if (i == j) {
						cov.set(i, i, 1.0);
					} else {
						if (cov.get(i, j) > 1.0) {
							cov.set(i, j, 1.0);
						} else if (cov.get(i, j) < -1.0) {
							cov.set(i, j, -1.0);
						}
					}
				}
			}
		}

		CorrelationResult result = new CorrelationResult(cov, colNames);

		return result;
	}

	/**
	 * when calculate covariance(x,y), if x or y is null, it will not involved in calculate.
	 */
	@Override
	public DenseMatrix covariance() {
		if (outerProduct == null) {
			return null;
		}
		int nStat = numMissingValue.size();

		double[][] cov = new double[nStat][nStat];
		for (int i = 0; i < nStat; i++) {
			for (int j = 0; j < nStat; j++) {
				cov[i][j] = Double.NaN;
			}
		}
		for (int i = 0; i < numericalColIndices.length; i++) {
			int idxI = numericalColIndices[i];
			for (int j = i; j < numericalColIndices.length; j++) {
				int idxJ = numericalColIndices[j];
				double count = xyCount.get(i, j);
				double d = outerProduct.get(i, j);
				d = (d - xSum.get(i, j) * xSum.get(j, i) / count) / (count - 1);
				cov[idxI][idxJ] = d;
				cov[idxJ][idxI] = d;
			}
		}
		return new DenseMatrix(cov);
	}

	/**
	 *
	 */
	TableSummarizer copy() {
		TableSummarizer srt = new TableSummarizer();
		srt.colNames = colNames.clone();
		srt.count = count;
		srt.numericalColIndices = numericalColIndices.clone();
		if (count != 0) {
			srt.numMissingValue = numMissingValue.clone();
			srt.sum = sum.clone();
			srt.squareSum = squareSum.clone();
			srt.sum3 = sum3.clone();
			srt.normL1 = normL1.clone();
			srt.min = min.clone();
			srt.max = max.clone();
		}

		if (outerProduct != null) {
			srt.outerProduct = outerProduct.clone();
			srt.xSum = xSum.clone();
			srt.xSquareSum = xSquareSum.clone();
			srt.xyCount = xyCount.clone();
		}

		return srt;
	}
}
