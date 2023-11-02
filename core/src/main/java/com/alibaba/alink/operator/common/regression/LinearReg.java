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
public class LinearReg {

	/**
	 * *
	 * 线性回归训练
	 *
	 * @param srt   数据表的基本统计结果
	 * @param nameY 因变量名称
	 * @param nameX 自变量名称
	 * @return 线性回归模型
	 * @throws Exception
	 */
	public static LinearRegressionModel train(SummaryResultTable srt, String nameY, String[] nameX) {
		if (srt == null) {
			throw new RuntimeException("srt must not null!");
		}
		String[] colNames = srt.colNames;
		Class[] types = new Class[colNames.length];
		for (int i = 0; i < colNames.length; i++) {
			types[i] = srt.col(i).dataType;
		}
		int indexY = TableUtil.findColIndexWithAssertAndHint(colNames, nameY);
		Class typeY = types[indexY];
		if (typeY != Double.class && typeY != Long.class && typeY != Integer.class) {
			throw new RuntimeException("col type must be double or bigint!");
		}
		if (nameX.length == 0) {
			throw new RuntimeException("nameX must input!");
		}
		for (int i = 0; i < nameX.length; i++) {
			int indexX = TableUtil.findColIndexWithAssertAndHint(colNames, nameX[i]);
			Class typeX = types[indexX];
			if (typeX != Double.class && typeX != Long.class && typeX != Integer.class) {
				throw new RuntimeException("col type must be double or bigint!");
			}
		}
		int nx = nameX.length;
		int[] indexX = new int[nx];
		for (int i = 0; i < nx; i++) {
			indexX[i] = TableUtil.findColIndexWithAssert(srt.colNames, nameX[i]);
		}

		return train(srt, indexY, indexX, nameY, nameX);
	}

	/**
	 * *
	 * 加载线性回归模型
	 *
	 * @param inputModelTableName 模型表
	 * @return 线性回归模型
	 * @throws Exception
	 */
	public static LinearRegressionModel loadModel(String inputModelTableName) throws Exception {
		//        if (!isModel(inputModelTableName)) {
		//            throw new Exception("model must be  linear regression model!");
		//        }
		//        OdpsTable otable = new OdpsTable(inputModelTableName);
		//        int count = (int) otable.getRecordCount();
		//        long nRecord = 0;
		//        String nameY;
		//        String[] nameX = new String[count - 2];
		//        double[] beta = new double[count - 1];
		//
		//        OdpsTableInputStream xis = new OdpsTableInputStream(otable);
		//        ArrayList r = xis.getRecordTemplate();
		//        xis.read(r);
		//        beta[0] = (Double) r.get(1);
		//        for (int i = 0; i < count - 2; i++) {
		//            xis.read(r);
		//            beta[i + 1] = (Double) r.get(1);
		//            nameX[i] = (String) r.get(0);
		//        }
		//        xis.read(r);
		//        nameY = (String) r.get(0);
		//        xis.close();
		//        LinearRegressionModel lrm = new LinearRegressionModel(nRecord, nameY, nameX);
		//        lrm.beta = beta;
		//        return lrm;
		return null;
	}

	/**
	 * *
	 * 线性回归预测
	 *
	 * @param model                    线性回归模型
	 * @param predictTableName         预测输入表
	 * @param selectedPartitions       预测输入表的分区
	 * @param appendColNames           输出表添加预测表的列名
	 * @param resultTableName          输出表
	 * @param resultTablePartitionName 输出表的分区
	 * @return
	 * @throws Exception
	 */
	public static void predict(LinearRegressionModel model, String predictTableName, String[] selectedPartitions,
							   String[] appendColNames, String resultTableName, String resultTablePartitionName)
		throws Exception {

	}

	static void write(LinearRegressionModel model, String outModelTableName) throws Exception {
		//        String[] colNames = new String[]{"colName", "coefficient"};
		//        Class[] types = new Class[]{String.class, Double.class};
		//        Object[][] data = new Object[model.nameX.size + 2][2];
		//        data[0][0] = "constant term";
		//        data[0][1] = model.beta[0];
		//        for (int i = 0; i < model.nameX.size; i++) {
		//            data[i + 1][0] = model.nameX[i];
		//            data[i + 1][1] = model.beta[i + 1];
		//        }
		//        data[model.nameX.size + 1][0] = model.nameY;
		//        data[model.nameX.size + 1][1] = 0.0;
		//        MTable mt = new MTable(colNames, types, data);
		//        boolean res = OdpsTableWriter.write(outModelTableName, mt);
		//        if (!res) {
		//            throw new Exception("write model error!");
		//        }
		//        setModelMeta(outModelTableName);
	}

	static LinearRegressionModel train(SummaryResultTable srt, int indexY, int[] indexX) throws Exception {
		int nx = indexX.length;
		String[] nameX = new String[nx];
		for (int i = 0; i < nx; i++) {
			nameX[i] = srt.colNames[indexX[i]];
		}
		String nameY = srt.colNames[indexY];
		return train(srt, indexY, indexX, nameY, nameX);
	}

	private static LinearRegressionModel train(SummaryResultTable srt, int indexY, int[] indexX, String nameY,
											   String[] nameX)  {
		//check if has missing value or nan value
		if (srt.col(indexY).countMissValue > 0 || srt.col(indexY).countNanValue > 0) {
			throw new RuntimeException("col " + nameY + " has null value or nan value!");
		}
		for (int i = 0; i < indexX.length; i++) {
			if (srt.col(indexX[i]).countMissValue > 0 || srt.col(indexX[i]).countNanValue > 0) {
				throw new RuntimeException("col " + nameX[i] + " has null value or nan value!");
			}
		}

		if (srt.col(0).countTotal == 0) {
			throw new RuntimeException("table is empty!");
		}
		if (srt.col(0).countTotal < nameX.length) {
			throw new RuntimeException("record size Less than features size!");
		}

		int nx = indexX.length;
		long N = srt.col(indexY).count;
		if (N == 0) {
			throw new RuntimeException("Y valid value num is zero!");
		}

		ArrayList <String> nameXList = new ArrayList <String>();
		for (int i = 0; i < indexX.length; i++) {
			if (srt.col(indexX[i]).count != 0) {
				nameXList.add(nameX[i]);
			}
		}
		nameXList.toArray(nameX);
		LinearRegressionModel lrr = new LinearRegressionModel(N, nameY, nameX);

		double[] XBar = new double[nx];
		for (int i = 0; i < nx; i++) {
			XBar[i] = srt.col(indexX[i]).mean();
		}
		double yBar = srt.col(indexY).mean();

		double[][] cov = srt.getCov();
		DenseMatrix A = new DenseMatrix(nx, nx);
		for (int i = 0; i < nx; i++) {
			for (int j = 0; j < nx; j++) {
				A.set(i, j, cov[indexX[i]][indexX[j]]);
			}
		}
		DenseMatrix C = new DenseMatrix(nx, 1);
		for (int i = 0; i < nx; i++) {
			C.set(i, 0, cov[indexX[i]][indexY]);
		}

		DenseMatrix BetaMatrix = A.solveLS(C);

		double d = yBar;
		for (int i = 0; i < nx; i++) {
			lrr.beta[i + 1] = BetaMatrix.get(i, 0);
			d -= XBar[i] * lrr.beta[i + 1];
		}
		lrr.beta[0] = d;

		double S = srt.col(nameY).variance() * (srt.col(nameY).count - 1);
		double alpha = lrr.beta[0] - yBar;
		double U = 0.0;
		U += alpha * alpha * N;
		for (int i = 0; i < nx; i++) {
			U += 2 * alpha * srt.col(indexX[i]).sum * lrr.beta[i + 1];
		}
		for (int i = 0; i < nx; i++) {
			for (int j = 0; j < nx; j++) {
				U += lrr.beta[i + 1] * lrr.beta[j + 1] * (cov[indexX[i]][indexX[j]] * (N - 1) + srt.col(indexX[i])
					.mean() * srt.col(indexX[j]).mean() * N);
			}
		}

		lrr.SST = S;
		lrr.SSR = U;
		lrr.SSE = S - U;
		lrr.dfSST = N - 1;
		lrr.dfSSR = nx;
		lrr.dfSSE = N - nx - 1;
		lrr.R2 = Math.max(0.0, Math.min(1.0, lrr.SSR / lrr.SST));
		lrr.R = Math.sqrt(lrr.R2);
		lrr.MST = lrr.SST / lrr.dfSST;
		lrr.MSR = lrr.SSR / lrr.dfSSR;
		lrr.MSE = lrr.SSE / lrr.dfSSE;
		lrr.Ra2 = 1 - lrr.MSE / lrr.MST;
		lrr.s = Math.sqrt(lrr.MSE);
		lrr.F = lrr.MSR / lrr.MSE;
		if (lrr.F < 0) {
			lrr.F = 0;
		}
		lrr.AIC = N * Math.log(lrr.SSE) + 2 * nx;

		A.scaleEqual(N - 1);
		//        DenseMatrix invA = A.Inverse();
		DenseMatrix invA = A.solveLS(JMatrixFunc.identity(A.numRows(), A.numRows()));

		for (int i = 0; i < nx; i++) {
			lrr.FX[i] = lrr.beta[i + 1] * lrr.beta[i + 1] / (lrr.MSE * invA.get(i, i));
			lrr.TX[i] = lrr.beta[i + 1] / (lrr.s * Math.sqrt(invA.get(i, i)));
		}

		try {
			int p = nameX.length;
			lrr.pEquation = 1 - CDF.F(lrr.F, p, N - p - 1);
			lrr.pX = new double[nx];
			for (int i = 0; i < nx; i++) {
				lrr.pX[i] = (1 - CDF.studentT(Math.abs(lrr.TX[i]), N - p - 1)) * 2;
			}
		} catch (Exception ex) {

		}
		return lrr;
	}

}
