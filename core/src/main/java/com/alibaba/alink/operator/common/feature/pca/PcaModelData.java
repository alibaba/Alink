package com.alibaba.alink.operator.common.feature.pca;

import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.feature.HasCalculationType;

public class PcaModelData implements AlinkSerializable {

	/**
	 * col names
	 */
	public String[] featureColNames;

	/**
	 * vector column name
	 */
	public String vectorColName;

	/**
	 * pca type
	 */
	public HasCalculationType.CalculationType pcaType;

	/**
	 * name of calculate cols
	 */
	public String[] nameX = null;

	/**
	 * mean of cols
	 */
	public double[] means = null;

	/**
	 * standard deviation of cols
	 */
	public double[] stddevs = null;

	/**
	 * number of principal component
	 */
	public int p;

	/**
	 * eigenvalues
	 */
	public double[] lambda;

	/**
	 * eigenvector
	 */
	public double[][] coef = null;

	/**
	 * covariance or correlation coefficient
	 **/
	public double[][] cov = null;

	/**
	 * col is the same value
	 */
	public Integer[] idxNonEqual = null;

	/**
	 * num of colnames
	 */
	public int nx;

	/**
	 * sum of lambda
	 */
	public double sumLambda;

	/**
	 * *
	 * calcultion principal component
	 *
	 * @param vec data
	 * @return principal component
	 */
	double[] calcPrinValue(double[] vec) {
		int nx = vec.length;
		double[] v = new double[nx];
		double[] r = new double[p];
		System.arraycopy(vec, 0, v, 0, nx);
		for (int k = 0; k < p; k++) {
			r[k] = 0;
			for (int i = 0; i < nx; i++) {
				r[k] += v[i] * coef[k][i];
			}
		}
		return r;
	}

	public String[] getCols() {
		return featureColNames;
	}

	public double[] getEigenValues() {
		return lambda;
	}

	public double[][] getEigenVectors() {
		return coef;
	}

	public double[] getProportions() {
		double[] proportions = new double[p];
		for (int i = 0; i < p; i++) {
			proportions[i] = lambda[i] / sumLambda;
		}
		return proportions;
	}

	public double[] getCumulatives() {
		double[] cumulatives = new double[p];
		for (int i = 0; i < p; i++) {
			if (i == 0) {
				cumulatives[i] = lambda[i];
			} else {
				cumulatives[i] = cumulatives[i - 1] + lambda[i];
			}
		}
		for (int i = 0; i < p; i++) {
			cumulatives[i] = cumulatives[i] / sumLambda;
		}
		return cumulatives;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder()
			.append(PrettyDisplayUtils.displayHeadline("PCA", '-'))
			.append("CalculationType: ").append(pcaType.name()).append("\n")
			.append("Number of Principal Component: " + this.p + "\n")
			.append("\n")
			.append("EigenValues: \n");
		String[] colColNames = new String[] {"Prin", "Eigenvalue", "Proportion", "Cumulative"};
		double[] proportions = getProportions();
		double[] cumulatives = getCumulatives();

		Object[][] vals = new Object[p][4];
		for (int i = 0; i < p; i++) {
			vals[i][0] = "Prin" + (i + 1);
			vals[i][1] = lambda[i];
			vals[i][2] = proportions[i];
			vals[i][3] = cumulatives[i];
		}

		sbd.append(PrettyDisplayUtils.indentLines(
			PrettyDisplayUtils.displayTable(vals, p, 4, null,
				colColNames, null, 10, 10), 4));

		sbd.append("\n");
		sbd.append("\n");

		sbd.append("EigenVectors: \n");
		String[] vecColNames = new String[p + 1];
		vecColNames[0] = "colName";
		for (int i = 0; i < p; i++) {
			vecColNames[i + 1] = "Prin" + (i + 1);
		}

		Object[][] vecVals = new Object[nx][p + 1];
		if (featureColNames != null) {
			for (int j = 0; j < nx; j++) {
				vecVals[j][0] = featureColNames[j];
			}
		} else {
			for (int j = 0; j < nx; j++) {
				vecVals[j][0] = j;
			}
		}
		for (int i = 0; i < p; i++) {
			for (int j = 0; j < nx; j++) {
				vecVals[j][i + 1] = 0;
			}
			for (int j = 0; j < idxNonEqual.length; j++) {
				vecVals[idxNonEqual[j]][i + 1] = coef[i][j];
			}
		}

		sbd.append(PrettyDisplayUtils.indentLines(
			PrettyDisplayUtils.displayTable(vecVals,
				nx, p + 1, null, vecColNames,
				null, 50, 11),
			4));

		return sbd.toString();
	}
}
