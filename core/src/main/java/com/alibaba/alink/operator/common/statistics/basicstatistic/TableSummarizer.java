package com.alibaba.alink.operator.common.statistics.basicstatistic;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.statistics.statistics.BaseMeasureIterator;
import com.alibaba.alink.operator.common.statistics.statistics.BooleanMeasureIterator;
import com.alibaba.alink.operator.common.statistics.statistics.DateMeasureIterator;
import com.alibaba.alink.operator.common.statistics.statistics.NumberMeasureIterator;
import com.alibaba.alink.operator.common.statistics.statistics.StatisticsIteratorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * It is summary for table， it will calculate statistics and return TableSummary.
 * You can get statistics from Summary.
 */
public class TableSummarizer extends BaseSummarizer {

	private static final long serialVersionUID = 4588962274305185787L;
	/**
	 * col names.
	 */
	public String[] colNames;

	/**
	 * col types.
	 */
	TypeInformation <?>[] colTypes;

	/**
	 * simple statistics for a col.
	 */
	BaseMeasureIterator[] statIterators;

	/**
	 * col number.
	 */
	private int n;

	/**
	 * select col indices.
	 */
	private int[] colIndices;

	/**
	 * num of numerical cols, boolean cols and date cols.
	 */
	private int numberN;

	/**
	 * numerical col and boolean col indices:
	 * if col is numerical or boolean , it will calculate cov and corr.
	 */
	private int[] numericalColIndices;

	/**
	 * Intermediate variable which will used in Visit function.
	 */
	private Double[] vals;

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
	 * default constructor.
	 */
	private TableSummarizer() {
	}

	/**
	 * if col is numerical, it will calculate all statistics, otherwise only calculate count and numMissingValue.
	 * if calculateOuterProduct is false, outerProduct，xSum, xSquareSum, xyCount are not be used,
	 * these are for correlation and covariance.
	 */
	public TableSummarizer(TableSchema tableSchema, boolean calculateOuterProduct) {
		this(tableSchema, calculateOuterProduct, null);
	}

	public TableSummarizer(TableSchema tableSchema, boolean calculateOuterProduct, String[] selectedColNames) {
		if (null == selectedColNames) {
			this.colNames = tableSchema.getFieldNames();
			this.n = this.colNames.length;
			this.colIndices = new int[this.n];
			for (int i = 0; i < this.n; i++) {
				this.colIndices[i] = i;
			}
		} else {
			this.colNames = selectedColNames;
			this.n = this.colNames.length;
			this.colIndices = TableUtil.findColIndicesWithAssertAndHint(tableSchema, selectedColNames);
		}
		this.colTypes = new TypeInformation <?>[this.n];
		for (int i = 0; i < this.n; i++) {
			this.colTypes[i] = tableSchema.getFieldType(this.colIndices[i]).get();
		}
		this.calculateOuterProduct = calculateOuterProduct;
		this.numericalColIndices = calcCovColIndices(this.colIndices, this.colTypes);
		this.numberN = this.numericalColIndices.length;
	}

	/**
	 * given row, incremental calculate statistics.
	 */
	public BaseSummarizer visit(Row row) {
		if (count == 0) {
			init();
		}

		count++;

		for (int i = 0; i < this.n; i++) {
			this.statIterators[i].visit(row.getField(this.colIndices[i]));
		}

		if (calculateOuterProduct) {
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

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder()
			.append("count: ")
			.append(count)
			.append("\n");
		if (count != 0) {
			for (int i = 0; i < this.n; i++) {
				sbd.append(this.colNames[i]).append(": ").append(this.statIterators[i]);
			}
		}

		return sbd.toString();
	}

	/**
	 * get summary result, you can get statistics from summary.
	 */
	public TableSummary toSummary() {
		TableSummary summary = new TableSummary();

		//summary.numericalColIndices = numericalColIndices;
		summary.numericalColIndices = new int[this.numberN];
		for (int i = 0; i < this.numberN; i++) {
			for (int k = 0; k < this.n; k++) {
				if (this.numericalColIndices[i] == this.colIndices[k]) {
					summary.numericalColIndices[i] = k;
					break;
				}
			}
		}
		summary.colNames = colNames;
		summary.count = count;
		summary.sum = new DenseVector(this.numberN);
		summary.sum2 = new DenseVector(this.numberN);
		summary.sum3 = new DenseVector(this.numberN);
		summary.sum4 = new DenseVector(this.numberN);
		summary.normL1 = new DenseVector(this.numberN);
		summary.minDouble = new DenseVector(this.numberN);
		summary.maxDouble = new DenseVector(this.numberN);
		summary.numMissingValue = new long[this.n];
		summary.min = new Object[this.numberN];
		summary.max = new Object[this.numberN];

		if (count > 0) {
			for (int i = 0; i < this.n; i++) {
				summary.numMissingValue[i] = this.statIterators[i].missingCount();
			}

			for (int i = 0; i < this.numberN; i++) {
				BaseMeasureIterator <?, ?> iterator = this.statIterators[summary.numericalColIndices[i]];
				if (iterator instanceof NumberMeasureIterator) {
					NumberMeasureIterator <?> numberIterator = (NumberMeasureIterator <?>) iterator;
					summary.sum.set(i, numberIterator.sum);
					summary.sum2.set(i, numberIterator.sum2);
					summary.sum3.set(i, numberIterator.sum3);
					summary.sum4.set(i, numberIterator.sum4);
					summary.minDouble.set(i,
						null == numberIterator.min ? Double.NaN : numberIterator.min.doubleValue());
					summary.maxDouble.set(i,
						null == numberIterator.max ? Double.NaN : numberIterator.max.doubleValue());
					summary.normL1.set(i, numberIterator.normL1);
					summary.min[i] = numberIterator.min;
					summary.max[i] = numberIterator.max;
				} else if (iterator instanceof BooleanMeasureIterator) {
					BooleanMeasureIterator boolIterator = (BooleanMeasureIterator) iterator;
					summary.sum.set(i, boolIterator.countTrue);
					summary.sum2.set(i, boolIterator.countTrue);
					summary.sum3.set(i, boolIterator.countTrue);
					summary.sum4.set(i, boolIterator.countTrue);
					summary.normL1.set(i, boolIterator.countTrue);
					summary.minDouble.set(i, boolIterator.countFalse > 0 ? 0.0 : 1.0);
					summary.maxDouble.set(i, boolIterator.countTrue > 0 ? 1.0 : 0.0);
					summary.min[i] = boolIterator.countFalse <= 0;
					summary.max[i] = boolIterator.countTrue > 0;
				} else if (iterator instanceof DateMeasureIterator) {
					DateMeasureIterator <?> dateIterator = (DateMeasureIterator <?>) iterator;
					summary.sum.set(i, Double.NaN);
					summary.sum2.set(i, Double.NaN);
					summary.sum3.set(i, Double.NaN);
					summary.sum4.set(i, Double.NaN);
					summary.minDouble.set(i, null == dateIterator.min ? Double.NaN : dateIterator.min.getTime());
					summary.maxDouble.set(i, null == dateIterator.max ? Double.NaN : dateIterator.max.getTime());
					summary.min[i] = dateIterator.min;
					summary.max[i] = dateIterator.max;
				}
			}
		}

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

		double[][] cov = new double[this.n][this.n];
		for (int i = 0; i < this.n; i++) {
			for (int j = 0; j < this.n; j++) {
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
	 * clone.
	 */
	TableSummarizer copy() {
		TableSummarizer srt = new TableSummarizer();
		srt.colNames = this.colNames.clone();
		srt.count = this.count;
		srt.n = this.n;
		srt.numberN = this.numberN;

		if (this.numericalColIndices != null) {
			srt.numericalColIndices = this.numericalColIndices.clone();
		}

		if (this.colTypes != null) {
			srt.colTypes = this.colTypes.clone();
		}

		if (count != 0) {
			srt.colIndices = this.colIndices.clone();
			srt.statIterators = new BaseMeasureIterator[this.n];
			for (int i = 0; i < this.n; i++) {
				srt.statIterators[i] = this.statIterators[i].clone();
			}
		}

		if (this.outerProduct != null) {
			srt.outerProduct = this.outerProduct.clone();
			srt.xSum = this.xSum.clone();
			srt.xSquareSum = this.xSquareSum.clone();
			srt.xyCount = this.xyCount.clone();
		}

		return srt;
	}

	/**
	 * n is the number of columns participating in the calculation.
	 */
	private void init() {
		this.statIterators = new BaseMeasureIterator[this.n];
		for (int i = 0; i < this.n; i++) {
			this.statIterators[i] = StatisticsIteratorFactory.getMeasureIterator(colTypes[i]);
		}

		if (calculateOuterProduct) {
			vals = new Double[numberN];
			outerProduct = new DenseMatrix(numberN, numberN);
			xSum = new DenseMatrix(numberN, numberN);
			xSquareSum = new DenseMatrix(numberN, numberN);
			xyCount = new DenseMatrix(numberN, numberN);
		}
	}

	/**
	 * col indices which col type is number or boolean or date.
	 */
	private static int[] calcCovColIndices(int[] colIndexes, TypeInformation <?>[] colTypes) {
		List <Integer> indicesList = new ArrayList <>();
		for (int i = 0; i < colIndexes.length; i++) {
			TypeInformation <?> type = colTypes[i];
			if (TableUtil.isSupportedNumericType(type)
				|| TableUtil.isSupportedBoolType(type)
				|| TableUtil.isSupportedDateType(type)) {
				indicesList.add(colIndexes[i]);
			}
		}
		return indicesList.stream().mapToInt(Integer::valueOf).toArray();
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

		if (left.n != right.n) {
			throw new AkIllegalStateException("left stat cols is not equal with right stat cols");
		}

		for (int i = 0; i < left.n; i++) {
			left.statIterators[i].merge(right.statIterators[i]);
		}

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
}
