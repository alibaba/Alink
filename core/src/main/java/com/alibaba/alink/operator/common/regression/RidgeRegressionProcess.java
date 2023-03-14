package com.alibaba.alink.operator.common.regression;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.jama.JMatrixFunc;
import com.alibaba.alink.common.probabilistic.CDF;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;

import java.util.ArrayList;

/**
 * @author yangxu
 */
public class RidgeRegressionProcess {

	private String nameY = null;
	private String[] nameX = null;
	private SummaryResultTable srt = null;

	public RidgeRegressionProcess(SummaryResultTable srt, String nameY, String[] nameX) {
		this.srt = srt;
		this.nameY = nameY;
		if (null != nameX) {
			this.nameX = new String[nameX.length];
			System.arraycopy(nameX, 0, this.nameX, 0, nameX.length);
		}
	}

	public static RidgeRegressionProcessResult calc(SummaryResultTable srt, String nameY, String[] nameX,
													double[] kVals) throws Exception {
		RidgeRegressionProcess rrp = new RidgeRegressionProcess(srt, nameY, nameX);
		return rrp.calc(kVals);
	}

	public RidgeRegressionProcessResult calc(double[] kVals) throws Exception {

		//////////////////////////////////////////////////////////////
		String[] colNames = srt.colNames;
		Class[] types = new Class[colNames.length];
		for (int i = 0; i < colNames.length; i++) {
			types[i] = srt.col(i).dataType;
		}
		int indexY = TableUtil.findColIndexWithAssertAndHint(colNames, nameY);
		Class typeY = types[indexY];
		if (typeY != Double.class && typeY != Long.class && typeY != Integer.class) {
			throw new Exception("col type must be double or bigint!");
		}
		if (nameX.length == 0) {
			throw new Exception("nameX must input!");
		}
		for (int i = 0; i < nameX.length; i++) {
			int indexX = TableUtil.findColIndexWithAssertAndHint(colNames, nameX[i]);
			Class typeX = types[indexX];
			if (typeX != Double.class && typeX != Long.class && typeX != Integer.class) {
				throw new Exception("col type must be double or bigint!");
			}
		}
		int nx = nameX.length;
		int[] indexX = new int[nx];
		for (int i = 0; i < nx; i++) {
			indexX[i] = TableUtil.findColIndexWithAssert(srt.colNames, nameX[i]);
		}

		//////////////////////////////////////////////////////////////
		if (srt.col(indexY).countMissValue > 0 || srt.col(indexY).countNanValue > 0) {
			throw new Exception("col " + nameY + " has null value or nan value!");
		}
		for (int i = 0; i < indexX.length; i++) {
			if (srt.col(indexX[i]).countMissValue > 0 || srt.col(indexX[i]).countNanValue > 0) {
				throw new Exception("col " + nameX[i] + " has null value or nan value!");
			}
		}

		if (srt.col(0).countTotal == 0) {
			throw new Exception("table is empty!");
		}
		if (srt.col(0).countTotal < nameX.length) {
			throw new Exception("record size Less than features size!");
		}

		long N = srt.col(indexY).count;
		if (N == 0) {
			throw new Exception("Y valid value num is zero!");
		}

		ArrayList <String> nameXList = new ArrayList <String>();
		for (int i = 0; i < indexX.length; i++) {
			if (srt.col(indexX[i]).count != 0) {
				nameXList.add(nameX[i]);
			}
		}
		nameXList.toArray(nameX);

		double[] XBar = new double[nx];
		for (int i = 0; i < nx; i++) {
			XBar[i] = srt.col(indexX[i]).mean();
		}
		double yBar = srt.col(indexY).mean();

		double[][] cov = srt.getCov();

		DenseMatrix C = new DenseMatrix(nx, 1);
		for (int i = 0; i < nx; i++) {
			C.set(i, 0, cov[indexX[i]][indexY]);
		}

		RidgeRegressionProcessResult ridgeResult = new RidgeRegressionProcessResult(kVals.length);

		for (int k = 0; k < kVals.length; k++) {
			double kval = kVals[k];
			ridgeResult.kVals[k] = kval;
			//            System.out.println(kval);

			ridgeResult.lrModels[k] = new LinearRegressionModel(N, nameY, nameX);

			DenseMatrix A = new DenseMatrix(nx, nx);
			for (int i = 0; i < nx; i++) {
				for (int j = 0; j < nx; j++) {
					if (i == j) {
						A.set(i, j, cov[indexX[i]][indexX[j]] + kval);
						//                        A.set(i, j, cov[indexX[i]][indexX[j]] + kval / (N - 1));
					} else {
						A.set(i, j, cov[indexX[i]][indexX[j]]);
					}
				}
			}

			DenseMatrix BetaMatrix = A.solveLS(C);

			double d = yBar;
			for (int i = 0; i < nx; i++) {
				ridgeResult.lrModels[k].beta[i + 1] = BetaMatrix.get(i, 0);
				d -= XBar[i] * ridgeResult.lrModels[k].beta[i + 1];
			}
			ridgeResult.lrModels[k].beta[0] = d;

			double S = srt.col(nameY).variance() * (srt.col(nameY).count - 1);
			double alpha = ridgeResult.lrModels[k].beta[0] - yBar;
			double U = 0.0;
			U += alpha * alpha * N;
			for (int i = 0; i < nx; i++) {
				U += 2 * alpha * srt.col(indexX[i]).sum * ridgeResult.lrModels[k].beta[i + 1];
			}
			for (int i = 0; i < nx; i++) {
				for (int j = 0; j < nx; j++) {
					U += ridgeResult.lrModels[k].beta[i + 1] * ridgeResult.lrModels[k].beta[j + 1] * (
						cov[indexX[i]][indexX[j]] * (N - 1) + srt.col(indexX[i]).mean() * srt.col(indexX[j]).mean()
							* N);
				}
			}

			ridgeResult.lrModels[k].SST = S;
			ridgeResult.lrModels[k].SSR = U;
			ridgeResult.lrModels[k].SSE = S - U;
			ridgeResult.lrModels[k].dfSST = N - 1;
			ridgeResult.lrModels[k].dfSSR = nx;
			ridgeResult.lrModels[k].dfSSE = N - nx - 1;
			ridgeResult.lrModels[k].R2 = Math.max(0.0,
				Math.min(1.0, ridgeResult.lrModels[k].SSR / ridgeResult.lrModels[k].SST));
			ridgeResult.lrModels[k].R = Math.sqrt(ridgeResult.lrModels[k].R2);
			ridgeResult.lrModels[k].MST = ridgeResult.lrModels[k].SST / ridgeResult.lrModels[k].dfSST;
			ridgeResult.lrModels[k].MSR = ridgeResult.lrModels[k].SSR / ridgeResult.lrModels[k].dfSSR;
			ridgeResult.lrModels[k].MSE = ridgeResult.lrModels[k].SSE / ridgeResult.lrModels[k].dfSSE;
			ridgeResult.lrModels[k].Ra2 = 1 - ridgeResult.lrModels[k].MSE / ridgeResult.lrModels[k].MST;
			ridgeResult.lrModels[k].s = Math.sqrt(ridgeResult.lrModels[k].MSE);
			ridgeResult.lrModels[k].F = ridgeResult.lrModels[k].MSR / ridgeResult.lrModels[k].MSE;
			if (ridgeResult.lrModels[k].F < 0) {
				ridgeResult.lrModels[k].F = 0;
			}
			ridgeResult.lrModels[k].AIC = N * Math.log(ridgeResult.lrModels[k].SSE) + 2 * nx;

			A.scaleEqual(N - 1);
			//        DenseMatrix invA = A.Inverse();
			DenseMatrix invA = A.solveLS(JMatrixFunc.identity(A.numRows(), A.numRows()));

			for (int i = 0; i < nx; i++) {
				ridgeResult.lrModels[k].FX[i] =
					ridgeResult.lrModels[k].beta[i + 1] * ridgeResult.lrModels[k].beta[i + 1] / (
						ridgeResult.lrModels[k].MSE * invA.get(i, i));
				ridgeResult.lrModels[k].TX[i] = ridgeResult.lrModels[k].beta[i + 1] / (ridgeResult.lrModels[k].s * Math
					.sqrt(invA.get(i, i)));
			}

			int p = nameX.length;
			ridgeResult.lrModels[k].pEquation = 1 - CDF.F(ridgeResult.lrModels[k].F, p, N - p - 1);
			ridgeResult.lrModels[k].pX = new double[nx];
			for (int i = 0; i < nx; i++) {
				ridgeResult.lrModels[k].pX[i] = (1 - CDF.studentT(Math.abs(ridgeResult.lrModels[k].TX[i]), N - p - 1))
					* 2;
			}

		}
		return ridgeResult;
	}
}
