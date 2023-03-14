package com.alibaba.alink.operator.common.regression;

/**
 * @author yangxu
 */
public class RidgeRegressionProcessResult {

	public int n;
	public double[] kVals = null;
	public LinearRegressionModel[] lrModels = null;

	public RidgeRegressionProcessResult(int n) {
		if (n <= 0) {
			throw new RuntimeException();
		}
		this.n = n;
		this.kVals = new double[n];
		this.lrModels = new LinearRegressionModel[n];
	}

	@Override
	public String toString() {
		java.io.CharArrayWriter cw = new java.io.CharArrayWriter();
		java.io.PrintWriter pw = new java.io.PrintWriter(cw, true);
		if (lrModels[0].nameX.length > 0) {
			pw.print(" K \t\t Intercept");
			for (int i = 0; i < lrModels[0].nameX.length; i++) {
				pw.print("\t\t " + lrModels[0].nameX[i]);
			}
			pw.println();
		}
		for (int k = 0; k < kVals.length; k++) {
			pw.print(kVals[k]);
			pw.print("\t");
			for (double betaVal : lrModels[k].beta) {
				pw.print(betaVal);
				pw.print("\t");
			}
			pw.println();
		}
		pw.println();
		return cw.toString();
	}

	public boolean saveLinearRegressionModel(double kVal, String outModelTableName) throws Exception {
		boolean bSaved = false;
		for (int k = 0; k < kVals.length; k++) {
			if (kVals[k] == kVal) {
				LinearReg.write(lrModels[k], outModelTableName);
				bSaved = true;
				break;
			}
		}
		return bSaved;
	}

	public boolean saveLinearRegressionModel(int kValIndex, String outModelTableName) throws Exception {
		if (kValIndex < 0 || kValIndex > lrModels.length) {
			throw new Exception("kVal not exists");
		}
		LinearReg.write(lrModels[kValIndex], outModelTableName);
		return true;
	}

}
