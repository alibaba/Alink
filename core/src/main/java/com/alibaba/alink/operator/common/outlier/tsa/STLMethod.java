package com.alibaba.alink.operator.common.outlier.tsa;

import com.alibaba.alink.operator.common.outlier.CalcMidian;
import com.alibaba.alink.operator.common.outlier.TimeSeriesAnomsUtils;

import java.util.Arrays;

//todo compare params with the paper.
public class STLMethod {
	//jump 至少为1以增加相应平滑器的速度。 线性插值发生在每jump个值之间。
	//t(trend), s(seasonal), l(loess)
	//提取季节性时的loess算法时间窗口宽度，需要是奇数
	private int sWindow;
	//提取季节性时局部拟合多项式的阶数，0或1
	private int sDegree = 0;
	private int sJump;
	//提取趋势时的loess算法时间窗口宽度，需要是奇数
	private int tWindow;
	private int tJump;
	//提取趋势性时局部拟合多项式的阶数，0或1
	private int tDegree = 1;
	//提取低通滤波子序列时的loess算法时间窗口宽度
	private int lWindow;
	private int lJump;
	//提取低通滤波子序列时局部拟合多项式的阶数，必须0或1
	private int lDegree;
	private int inner;//the iter times of inner iter.
	private int outer;
	public double[] trend;
	public double[] seasonal;
	public double[] remainder;

	//frequency应该是每个季节下的数据个数。
	public STLMethod(double[] ts, int freq, String sWindowString, int sDegree, Integer sJump,
					 Integer tWindow, Integer tJump, Integer tDegree,
					 Integer lWindow, Integer lJump, Integer lDegree,
					 boolean robust, Integer inner, Integer outer) {
		int n = ts.length;
		//frequency is the object numbers per period?
		if (freq < 2) {
			throw new RuntimeException("The frequency must be greater than 1.");
		}
		if (n <= freq * 2) {
			throw new RuntimeException(
				String.format(
					"The time series must contain more than 2 full periods of data. n：%s, freq: %s", n, freq));
		}
		if ("periodic".equals(sWindowString)) { // todo now only support periodic. What sWindow will be if else?
			sWindow = 10 * n + 1;
		}
		this.sDegree = sDegree;
		if (sJump == null) {
			this.sJump = new Double(Math.ceil((this.sWindow / 10.0))).intValue();
		} else {
			this.sJump = sJump;
		}
		if (tWindow == null) {
			this.tWindow = nextOdd(Math.ceil(1.5 * freq / (1 - 1.5 / sWindow)));
		} else {
			this.tWindow = tWindow;
		}
		if (tJump == null) {
			this.tJump = new Double(Math.ceil((this.tWindow / 10.0))).intValue();
		} else {
			this.tJump = tJump;
		}
		if (tDegree != null) {
			this.tDegree = tDegree;
		}
		if (lWindow == null) {
			this.lWindow = nextOdd(freq);
		} else {
			this.lWindow = lWindow;
		}
		if (lJump == null) {
			this.lJump = new Double(Math.ceil((this.lWindow / 10.0))).intValue();
		} else {
			this.lJump = lJump;
		}
		if (lDegree == null) {
			this.lDegree = this.tDegree;
		} else {
			this.lDegree = lDegree;
		}
		//robust是在loess过程中是否使用鲁棒拟合
		if (inner == null) {
			this.inner = robust ? 1 : 2;
		} else {
			this.inner = inner;
		}
		if (outer == null) {
			this.outer = robust ? 15 : 0;
		} else {
			this.outer = outer;
		}
		//initialize
		double[] weights = new double[n];
		double[] seasonal = new double[n];
		double[] trend = new double[n];
		this.remainder = new double[n];
		double[][] work = new double[5][n + (freq << 1)];

		this.sWindow = maxOdd(this.sWindow);
		this.tWindow = maxOdd(this.tWindow);
		this.lWindow = maxOdd(this.lWindow);
		//this function is inner loop of the paper.
		//useW就是计算过程中是否使用weight?
		stlstp(ts, n, freq, sWindow, this.tWindow, this.lWindow, this.sDegree, this.tDegree, this.lDegree,
			this.sJump, this.tJump, this.lJump, this.inner, false, weights, seasonal, trend, work);

		//does outer just calculate robust weights？
		for (int nullTemp = 0; nullTemp < this.outer; nullTemp++) {
			for (int i = 0; i < n; i++) {
				work[0][i] = trend[i] + seasonal[i];//fit的结果，没有考虑reminder
			}
			//work0 may be the result of fit, and abs(work0-ts) is reminder。this step updates robust weights.
			//this is due to the paper，是外层循环做的事情。
			stlrwt(ts, n, work[0], weights);
			//seems the inner loop。
			stlstp(ts, n, freq, sWindow, this.tWindow, this.lWindow, this.sDegree, this.tDegree, this.lDegree,
				this.sJump, this.tJump, this.lJump, this.inner, true, weights, seasonal, trend, work);
		}
		if (this.outer <= 0) {
			Arrays.fill(weights, 1.0);
		}
		for (int i = 0; i < n; i++) {
			this.remainder[i] = ts[i] - trend[i] - seasonal[i];
		}
		this.trend = trend;
		this.seasonal = seasonal;
	}

	private static int maxOdd(int value) {
		value = Math.max(value, 3);
		if (value % 2 == 0) {
			value += 1;
		}
		return value;
	}

	private static int nextOdd(double x) {
		int temp = (int) Math.round(x);
		if (temp % 2 == 0) {
			temp += 1;
		}
		return temp;
	}

	//itdeg is tDegree
	//this is the inner iteration.
	private static void stlstp(double[] y, int n, int np, int sWindow, int tWindow, int lWindow,
							   int sDegree, int itdeg, int lDegree,
							   int sJump, int tJump, int lJump,
							   int ni, boolean userW,
							   double[] weights, double[] seasonal, double[] trend, double[][] work) {
		//ni is inner
		for (int nullTemp = 0; nullTemp < ni; nullTemp++) {
			//first step: detrending. After detrending, save in work0.
			for (int i = 0; i < n; i++) {
				work[0][i] = y[i] - trend[i];//work0看上去是去除趋势的数据。
			}

			stlss(work[0], n, np, sWindow, sDegree, sJump, userW, weights, work[1], work[2], work[3], work[4],
				seasonal);
			stlfts(work[1], n + 2 * np, np, work[2], work[0]);
			stless(work[2], n, lWindow, lDegree, lJump, false, work[3], work[0], work[4]);
			for (int i = 0; i < n; i++) {
				//work1可能是平滑结果。
				seasonal[i] = work[1][np + i] - work[0][i];//可能是去除平滑周期子序列趋势
			}
			for (int i = 0; i < n; i++) {
				work[0][i] = y[i] - seasonal[i];//去周期？？
			}
			stless(work[0], n, tWindow, itdeg, tJump, userW, weights, trend, work[2]);
		}
	}

	//isdeg is sDegree. y is the data after minus trend，that is seasonal+reminder.
	private static void stlss(double[] y, int n, int np,
							  int sWindow, int isdeg, int sJump, boolean userW, double[] weights,
							  double[] season, double[] work1, double[] work2, double[] work3, double[] work4) {
		for (int j = 0; j < np; j++) {
			int k = (n - j - 1) / np + 1;//k应该是判断当前在哪个period中。
			for (int i = 0; i < k; i++) {
				work1[i] = y[i * np + j];//what is work1?
			}
			if (userW) {//the first loop is false，and then in outer is true了。
				for (int i = 0; i < k; i++) {
					work3[i] = weights[i * np + j];
				}
			}
			double[] work2From1 = Arrays.copyOfRange(work2, 1, work2.length);
			stless(work1, k, sWindow, isdeg, sJump, userW, work3, work2From1, work4);
			System.arraycopy(work2From1, 0, work2, 1, work2From1.length);
			int nRight = Math.min(sWindow, k);
			Double nVal = stlest(work1, k, sWindow, isdeg, 0, work2[0], 1, nRight, work4, userW, work3);
			if (nVal != null) {
				work2[0] = nVal;
			} else {
				work2[0] = work2[1];
			}
			int nLeft = Math.max(1, k - sWindow + 1);
			nVal = stlest(work1, k, sWindow, isdeg, k + 1, work2[k + 1], nLeft, k, work4, userW, work3);
			if (nVal != null) {
				work2[k + 1] = nVal;
			} else {
				work2[k + 1] = work2[k];
			}
			for (int m = 0; m < k + 2; m++) {
				season[m * np + j] = work2[m];
			}
		}
	}

	private static void stlfts(double[] x, int n, int np, double[] trend, double[] work) {
		stlma(x, n, np, trend);
		stlma(trend, n - np + 1, np, work);
		stlma(work, n - 2 * np + 2, 3, trend);
	}

	private static void stlma(double[] x, int n, int length, double[] ave) {
		double v = TimeSeriesAnomsUtils.sumArray(x, length);
		ave[0] = v / length;

		int newN = n - length + 1;
		if (newN > 1) {
			int k = length;
			int m = 0;
			for (int j = 1; j < newN; j++) {
				k += 1;
				m += 1;
				v = v - x[m - 1] + x[k - 1];
				ave[j] = v / length;
			}
		}
	}

	private static void stless(double[] y, int n, int length, int ideg,
							   int nJump, boolean userW, double[] weights, double[] ys, double[] res) {
		if (n < 2) {
			ys[0] = y[0];
			return;
		}
		int newNJump = Math.min(nJump, n - 1);
		int nLeft = 0;//ini
		int nRight = 0;//ini
		if (length >= n) {
			nLeft = 1;
			nRight = n;
			for (int i = 0; i < n; i += newNJump) {
				Double nys = stlest(y, n, length, ideg, i + 1, ys[i], nLeft, nRight, res, userW, weights);
				if (nys != null) {
					ys[i] = nys;
				} else {
					ys[i] = y[i];
				}
			}
		} else {
			if (newNJump == 1) {
				int nsh = (length + 1) / 2;
				nLeft = 1;
				nRight = length;
				for (int i = 0; i < n; i++) {
					if (i + 1 > nsh && nRight != n) {
						nLeft++;
						nRight++;
					}
					Double nys = stlest(y, n, length, ideg, i + 1, ys[i], nLeft, nRight, res, userW, weights);
					if (nys != null) {
						ys[i] = nys;
					} else {
						ys[i] = y[i];
					}
				}
			} else {
				int nsh = (length + 1) / 2;
				for (int i = 1; i < n + 1; i += newNJump) {
					if (i < nsh) {
						nLeft = 1;
						nRight = length;
					} else if (i >= (n - nsh + 1)) {
						nLeft = n - length + 1;
						nRight = n;
					} else {
						nLeft = i - nsh + 1;
						nRight = length + i - nsh;
					}
					Double nys = stlest(y, n, length, ideg, i, ys[i - 1], nLeft, nRight, res, userW, weights);
					if (nys != null) {
						ys[i - 1] = nys;
					} else {
						ys[i - 1] = y[i - 1];
					}
				}
			}
		}
		if (newNJump != 1) {
			double delta;
			for (int i = 0; i < n - newNJump; i += newNJump) {
				delta = (ys[i + newNJump] - ys[i]) * 1.0 / newNJump;
				for (int j = 1; j < newNJump; j++) {
					ys[i + j] = ys[i] + delta * j;
				}
			}
			int k = ((n - 1) / newNJump) * newNJump + 1;
			if (k != n) {
				Double nys = stlest(y, n, length, ideg, n, ys[n - 1], nLeft, nRight, res, userW, weights);
				if (nys != null) {
					ys[n - 1] = nys;
				} else {
					ys[n - 1] = y[n - 1];
				}
				if (k != n - 1) {
					delta = (ys[n - 1] - ys[k - 1]) * 1.0 / (n - k);
					for (int j = 0; j < n - 1 - k; j++) {
						ys[k + j] = ys[k - 1] + delta * (1 + j);
					}
				}
			}

		}
	}

	//todo fit only get the first n data.
	//weights is the robustness in the paper.
	//这个是外层训练做的事情
	private static void stlrwt(double[] y, int n, double[] fit, double[] weights) {
		double[] r = new double[n];
		for (int i = 0; i < n; i++) {
			r[i] = Math.abs(y[i] - fit[i]);
		}
		double median = 6 * CalcMidian.tempMedian(r);//r is the reminder.
		double lowThre = 0.001 * median;
		double highThre = 0.999 * median;
		for (int i = 0; i < n; i++) {
			if (r[i] <= lowThre) {
				weights[i] = 1;
			} else if (r[i] > highThre) {
				weights[i] = 0;
			} else {
				weights[i] = Math.pow(1 - Math.pow(r[i] / median, 2), 2);
			}
		}
	}

	private static Double stlest(double[] y, int n, int length, int ideg, int xs, double ys,
								 int nLeft, int nRight, double[] w, boolean userW, double[] weights) {
		int h = Math.max(xs - nLeft, nRight - xs);
		if (length > n) {
			h += (length - n) / 2;
		}
		int[] r = generateArange(nLeft - xs, nRight - xs + 1, Type.ABS);
		int[] window = generateArange(nLeft - 1, nRight, Type.SELF);
		int rLength = nRight + 1 - nLeft;
		double lowThre = 0.001 * h;
		double highThre = 0.999 * h;
		int[] judge = new int[rLength];
		for (int i = 0; i < rLength; i++) {
			if (r[i] <= lowThre) {
				judge[i] = 0;//low
			} else if (r[i] > highThre) {
				judge[i] = 1;//high
			} else {
				judge[i] = 2;//middle
			}
		}
		double a = 0;
		for (int i = 0; i < rLength; i++) {
			int num = judge[i];
			if (num == 0) {
				w[window[i]] = 1;
			} else if (num == 2) {
				w[window[i]] = Math.pow(1 - Math.pow(r[i] * 1.0 / h, 3), 3);//w is the neighborhood weight
			}
			if (num != 1) {
				if (userW) {
					w[window[i]] *= weights[window[i]];
				}
				a += w[window[i]];
			}
			if (num == 1) {
				w[window[i]] = 0;
			}
		}
		Double ret;
		if (a <= 0) {
			ret = null;
		} else {
			for (int i = nLeft - 1; i < nRight; i++) {
				w[i] /= a;
			}
			if (h > 0 && ideg > 0) {
				a = 0;
				for (int i = 0; i < rLength; i++) {
					a += w[nLeft - 1 + i] * (nLeft + i);
				}
				double b = xs - a;
				double c = 0;
				for (int i = 0; i < rLength; i++) {
					c += w[nLeft - 1 + i] * Math.pow(nLeft - a + i, 2);
				}
				if (Math.sqrt(c) > 0.001 * (n - 1)) {
					b /= c;
					for (int i = 0; i < rLength; i++) {
						w[nLeft - 1 + i] *= (b * (nLeft - a + i) + 1);
					}
				}
			}
			ret = 0.;
			for (int i = 0; i < rLength; i++) {
				ret += w[nLeft - 1 + i] * y[nLeft - 1 + i];
			}
		}
		return ret;
	}

	//arange
	private static int[] generateArange(int start, int end, Type type) {
		int length = end - start;
		int[] res = new int[length];
		if (type == Type.ABS) { //abs
			for (int i = 0; i < length; i++) {
				res[i] = Math.abs(start + i);
			}
		} else if (type == Type.SQUARE) { //square
			for (int i = 0; i < length; i++) {
				res[i] = (int) Math.pow(start + i, 2);
			}
		} else if (type == Type.SELF) {
			for (int i = 0; i < length; i++) {
				res[i] = start + i;
			}
		}
		return res;
	}

	enum Type {
		/**
		 * calculate the absolute value
		 */
		ABS,
		/**
		 * calculate the square value
		 */
		SQUARE,
		/**
		 * calculate itself
		 */
		SELF
	}
}
