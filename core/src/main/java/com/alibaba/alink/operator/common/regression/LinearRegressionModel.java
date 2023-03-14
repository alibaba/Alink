package com.alibaba.alink.operator.common.regression;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.utils.JsonConverter;

/**
 * @author yangxu
 */
@NameCn("线性回归模型")
public class LinearRegressionModel implements RegressionModelInterface {

	/**
	 * *
	 * 因变量名称
	 */
	public String nameY;
	/**
	 * *
	 * 自变量名称
	 */
	public String[] nameX = new String[0];
	/**
	 * *
	 * 记录的总个数
	 */
	public long n;
	/**
	 * *
	 * 总离差平方和
	 */
	public double SST;
	/**
	 * *
	 * 回归平方和
	 */
	public double SSR;
	/**
	 * *
	 * 剩余平方和
	 */
	public double SSE;
	/**
	 * *
	 * 总离差平方和的自由度
	 */
	public double dfSST;
	/**
	 * *
	 * 回归平方和的自由度
	 */
	public double dfSSR;
	/**
	 * *
	 * 剩余平方和的自由度
	 */
	public double dfSSE;
	/**
	 * *
	 * 多重判定系数
	 */
	public double R2;
	/**
	 * *
	 * 多重相关系数
	 */
	public double R;
	/**
	 * *
	 * 修正的多重判定系数
	 */
	public double Ra2;
	/**
	 * *
	 * 总离差均方
	 */
	public double MST;
	/**
	 * *
	 * 回归均方
	 */
	public double MSR;
	/**
	 * *
	 * 剩余均方
	 */
	public double MSE;
	/**
	 * *
	 * 剩余标准差
	 */
	public double s;
	/**
	 * *
	 * 回归方程F检验值
	 */
	public double F;
	/**
	 * *
	 * 回归方程F检验的P-值
	 */
	public double pEquation;
	/**
	 * *
	 * 回归系数
	 */
	public double[] beta = null;
	/**
	 * *
	 * 各变量F检验值
	 */
	public double[] FX = null;
	/**
	 * *
	 * 各变量T检验值
	 */
	public double[] TX = null;
	/**
	 * *
	 * 各变量双侧T检验的P-值
	 */
	public double[] pX = null;
	/**
	 * *
	 * AIC信息统计量，Akaike Information Criterion AIC = n*Ln(SSE)+2*p
	 */
	public double AIC;

	public LinearRegressionModel(long nRecord, String nameY, String[] nameX) {
		int nx = nameX.length;
		n = nRecord;
		beta = new double[nx + 1];
		FX = new double[nx];
		TX = new double[nx];
		this.nameY = nameY;
		this.nameX = new String[nameX.length];
		System.arraycopy(nameX, 0, this.nameX, 0, nameX.length);
	}

	/**
	 * *
	 * 计算Cp统计量 Cp = ( n - m - 1 )*( SSEp / SSEm )- n + 2*( p + 1 )
	 *
	 * @param m    全部变量的个数
	 * @param SSEm 对于全部变量的误差和
	 * @return Cp统计量
	 */
	public double getCp(int m, double SSEm) {
		int p = nameX.length;
		if (p > m) {
			throw new RuntimeException();
		}
		return (n - m - 1) * this.SSE / SSEm - n + 2 * (p + 1);
	}

	public String toJson() {
		return JsonConverter.gson.toJson(this);
	}

	@Override
	public String toString() {
		int m = nameX.length;
		java.io.CharArrayWriter cw = new java.io.CharArrayWriter();
		java.io.PrintWriter pw = new java.io.PrintWriter(cw, true);
		if (nameX.length > 0) {
			pw.print(nameY + " = " + beta[0]);
			for (int i = 0; i < nameX.length; i++) {
				pw.print(" + " + beta[i + 1] + " * " + nameX[i]);
			}
			pw.println();
		}
		if (TX != null) {
			pw.print("RegCoef =");
			for (int i = 0; i <= m; i++) {
				pw.print(" \t");
				pw.print(beta[i]);
			}
			pw.println();
			pw.print("R  = ");
			pw.print(R);
			pw.print(" \tR2  = ");
			pw.print(R2);
			pw.print(" \tRa2  = ");
			pw.println(Ra2);
			pw.print("F  = ");
			pw.print(F);
			pw.print(" \tp_value = ");
			pw.println(this.pEquation);

			pw.print("FX =");
			for (int i = 0; i < m; i++) {
				pw.print(" \t");
				pw.print(FX[i]);
			}

			pw.println();

			pw.print("TX =");
			for (int i = 0; i < m; i++) {
				pw.print(" \t");
				pw.print(TX[i]);
			}

			pw.println();

			pw.print("pX =");
			for (int i = 0; i < m; i++) {
				pw.print(" \t");
				pw.print(pX[i]);
			}

		}
		pw.println();
		return cw.toString();
	}

}
