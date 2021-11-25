package com.alibaba.alink.common.probabilistic;

import java.util.HashMap;
import java.util.Random;

/**
 * @author yangxu
 */
public class XRandom implements java.io.Serializable {

	private static final long serialVersionUID = -3973714123611662398L;
	private Random rn = null;

	public XRandom() {
		rn = new Random();
	}

	public void setSeed(long seed) {
		rn.setSeed(seed);
	}

	public double nextDouble() {
		return rn.nextDouble();
	}

	public int nextInt() {
		return rn.nextInt();
	}

	public boolean bernoulli(double p) {
		if (p < 0 || p > 1) {
			throw new IllegalArgumentException("p must bewteen 0 and 1");
		}
		double d = rn.nextDouble();
		if (d < p) {
			return true;
		} else {
			return false;
		}
	}
	//return a random long integer between 0 (inclusive) and n (exclusive) with equally probability distribution

	public long nextLong(long n) {
		if (n <= 0) {
			throw new IllegalArgumentException("n must be positive");
		}
		double d = rn.nextDouble();
		return (long) Math.floor(d * (double) n);
	}
	//return #size random long integers between 0 (inclusive) and n (exclusive) with equally probability distribution
	//Para "replace" indicates if replace the integer

	public long[] randLongs(long n, int size, boolean replace) {
		if (n <= 0) {
			throw new IllegalArgumentException("n must be positive");
		}
		if (size <= 0) {
			throw new IllegalArgumentException("size must be positive");
		}
		if (replace) {
			long[] r = new long[size];
			for (int i = 0; i < r.length; i++) {
				r[i] = nextLong(n);
			}
			return r;
		} else {
			if (size > n) {
				throw new IllegalArgumentException("Sample size is larger than entire set size!");
			}
			long[] r = new long[size];
			HashMap <Long, Long> hm = new HashMap <Long, Long>();
			for (int i = 0; i < size; i++) {
				//                System.out.println("Real Array: ");
				//                for (long j = 0; j < n - i; j++) {
				//                    if (hm.containsKey(j)) {
				//                        System.out.print("M " + hm.get(j) + ", ");
				//                    } else {
				//                        System.out.print(j + ", ");
				//                    }
				//                }
				//                System.out.println("size: " + hm.size());
				long idx = nextLong(n - i);
				long oneRound = idx;
				if (hm.containsKey(idx)) {
					oneRound = hm.get(idx);
				}
				long lastOne = n - i - 1;
				if (idx == lastOne) {
					hm.remove(idx);
				} else {
					long val = lastOne;
					if (hm.containsKey(lastOne)) {
						val = hm.get(lastOne);
						hm.remove(lastOne);
					}
					hm.put(idx, val);
				}
				r[i] = oneRound;
			}
			return r;
		}

	}

	public int[] randInts(int n, int size, boolean replace, double[] para) throws Exception {
		if (n <= 0) {
			throw new IllegalArgumentException("n must be positive");
		}
		if (size <= 0) {
			throw new IllegalArgumentException("size must be positive");
		}
		if (para.length != n) {
			throw new IllegalArgumentException("size of para must be same as n");
		}
		if (!replace) {
			if (size > n) {
				throw new IllegalArgumentException("Sample size is larger than entire set size!");
			}
		}
		int[] r = new int[size];
		WeightedSampleTree wst = new WeightedSampleTree(para, this);
		for (int i = 0; i < r.length; i++) {
			r[i] = wst.getRandomNumber(replace);
		}
		return r;
	}

	public int[] randInts(int n, int size, boolean replace) {
		if (n <= 0) {
			throw new IllegalArgumentException("n must be positive");
		}
		if (size <= 0) {
			throw new IllegalArgumentException("size must be positive");
		}
		if (replace) {
			int[] r = new int[size];
			for (int i = 0; i < r.length; i++) {
				r[i] = nextInt(n);
			}
			return r;
		} else {
			if (size > n) {
				throw new IllegalArgumentException("Sample size is larger than entire set size!");
			}
			int[] r = new int[size];
			HashMap <Integer, Integer> hm = new HashMap <Integer, Integer>();
			for (int i = 0; i < size; i++) {
				int idx = nextInt(n - i);
				int oneRound = idx;
				if (hm.containsKey(idx)) {
					oneRound = hm.get(idx);
				}
				int lastOne = n - i - 1;
				if (idx == lastOne) {
					hm.remove(idx);
				} else {
					int val = lastOne;
					if (hm.containsKey(lastOne)) {
						val = hm.get(lastOne);
						hm.remove(lastOne);
					}
					hm.put(idx, val);
				}
				r[i] = oneRound;
			}
			return r;
		}

	}

	public int TestSize_randLong(long n, int size, boolean replace) {
		if (n <= 0) {
			throw new IllegalArgumentException("n must be positive");
		}
		if (replace) {
			long[] r = new long[size];
			for (int i = 0; i < r.length; i++) {
				r[i] = nextLong(n);
			}
			return 0;
		} else {
			if (size > n) {
				throw new IllegalArgumentException("Sample size is larger than entire set size!");
			}
			int reSize = 0;
			long[] r = new long[size];
			HashMap <Long, Long> hm = new HashMap <Long, Long>();

			for (int i = 0; i < size; i++) {
				//                System.out.println("Real Array: ");
				//                for (long j = 0; j < n - i; j++) {
				//                    if (hm.containsKey(j)) {
				//                        System.out.print("M " + hm.get(j) + ", ");
				//                    } else {
				//                        System.out.print(j + ", ");
				//                    }
				//                }
				//                System.out.println("size: " + hm.size());
				long idx = nextLong(n - i);
				long oneRound = idx;
				if (hm.containsKey(idx)) {
					oneRound = hm.get(idx);
				}
				long lastOne = n - i - 1;
				if (idx == lastOne) {
					hm.remove(idx);
				} else {
					long val = n - i - 1;
					if (hm.containsKey(lastOne)) {
						val = hm.get(lastOne);
						hm.remove(lastOne);
					}
					hm.put(idx, val);
				}
				//                System.out.println("OneRound: " + oneRound);
				reSize = Math.max(reSize, hm.size());
				r[i] = oneRound;
			}
			return reSize;
		}

	}

	public int nextInt(int n) {
		return rn.nextInt(n);
	}

	public float nextFloat() {
		return rn.nextFloat();
	}

	public long nextLong() {
		return rn.nextLong();
	}

	public boolean nextBoolean() {
		return rn.nextBoolean();
	}

	public void nextBytes(byte[] bytes) {
		rn.nextBytes(bytes);
	}

	public double nextGaussian() {
		return rn.nextGaussian();
	}

	//==================================
	// Continuous Distribution
	//==================================

	/***
	 * 产生半闭半开区间[0.0, 1.0)上均匀分布的随机数序列
	 *
	 * @param len   所要生成的随机数个数
	 * @return 产生的随机数序列
	 */
	public double[] uniformDistArray(int len) {
		double[] p = new double[len];
		int i;
		for (i = 0; i < len; i++) {
			p[i] = rn.nextDouble(); //drand48();
		}
		return p;
	}

	/***
	 * 产生开区间(0.0, 1.0)上均匀分布的随机数序列
	 *
	 * @param len   所要生成的随机数个数
	 * @return 产生的随机数序列
	 */
	public double[] uniformDistArray_OpenInterval(int len) {
		double[] p = new double[len];
		int i;
		double t;
		for (i = 0; i < len; i++) {
			t = rn.nextDouble();
			if (0.0 != t) {
				p[i] = t;
			} else {
				while (0.0 == t) {
					t = rn.nextDouble();
				}
				p[i] = t;
			}
		}
		return p;
	}

	/***
	 * 产生服从均匀分布的随机数序列
	 *
	 * @param len           所要生成的随机数个数
	 * @param lowerBound    均匀分布的下界
	 * @param upperBound    均匀分布的上界
	 * @return 产生的随机数序列
	 */
	public double[] uniformDistArray(int len, double lowerBound, double upperBound) {
		double[] p = uniformDistArray(len);
		int i;
		for (i = 0; i < len; i++) {
			p[i] = IDF.uniform(p[i], lowerBound, upperBound);
		}
		return p;
	}

	/***
	 * 产生服从指数分布的随机数序列
	 * 当x>=0时，概率密度函数 f(x) = lambda * exp( -lambda * x) ；否则f(x)=0
	 * 其中 lambda>0
	 *
	 * @param len   所要生成的随机数个数
	 * @param lambda 指数分布参数
	 * @return 产生的随机数序列
	 */
	public double[] exponentialDistArray(int len, double lambda) {
		double[] p = uniformDistArray(len);
		int i;
		for (i = 0; i < len; i++) {
			p[i] = IDF.exponential(p[i], lambda);
		}
		return p;
	}

	/***
	 * 产生服从正态分布的随机数序列
	 *
	 * @param len       所要生成的随机数个数
	 * @param mu        均值
	 * @param sigma2    方差
	 * @return 产生的随机数序列
	 */
	public double[] normalDistArray(int len, double mu, double sigma2) {
		double[] x = uniformDistArray_OpenInterval(len);
		for (int i = 0; i < len; i++) {
			x[i] = IDF.normal(x[i], mu, sigma2);
		}
		return x;
	}

	/***
	 * 产生服从F分布的随机数序列
	 *
	 * @param len       所要生成的随机数个数
	 * @param df1       自由度1
	 * @param df2       自由度2
	 * @return 产生的随机数序列
	 */
	public double[] FDistArray(int len, double df1, double df2) {
		double[] x = uniformDistArray(len);
		for (int i = 0; i < len; i++) {
			x[i] = IDF.F(x[i], df1, df2);
		}
		return x;
	}

	/***
	 * 产生服从学生T分布的随机数序列
	 *
	 * @param len       所要生成的随机数个数
	 * @param df        自由度
	 * @return 产生的随机数序列
	 */
	public double[] studentTDistArray(int len, double df) {
		double[] x = uniformDistArray_OpenInterval(len);
		for (int i = 0; i < len; i++) {
			x[i] = IDF.studentT(x[i], df);
		}
		return x;
	}

	/***
	 * 产生服从Gamma分布的随机数序列
	 *
	 * @param len       所要生成的随机数个数
	 * @param alpha     参数
	 * @param lambda    参数
	 * @return 产生的随机数序列
	 */
	public double[] gammaDistArray(int len, double alpha, double lambda) {
		double[] x = uniformDistArray(len);
		for (int i = 0; i < len; i++) {
			x[i] = IDF.gamma(x[i], alpha, lambda);
		}
		return x;
	}

	/***
	 * 产生服从卡方分布的随机数序列
	 *
	 * @param len       所要生成的随机数个数
	 * @param df        自由度
	 * @return 产生的随机数序列
	 */
	public double[] chi2DistArray(int len, double df) {
		double[] x = uniformDistArray(len);
		for (int i = 0; i < len; i++) {
			x[i] = IDF.chi2(x[i], df);
		}
		return x;
	}

	/***
	 * 产生服从Beta分布的随机数序列
	 *
	 * @param len       所要生成的随机数个数
	 * @param a         参数
	 * @param b         参数
	 * @return 产生的随机数序列
	 */
	public double[] betaDistArray(int len, double a, double b) {
		double[] x = new double[len];
		for (int i = 0; i < len; i++) {
			x[i] = rn.nextDouble();
			if (0 == x[i]) {
				x[i] = 1;
			} else {
				x[i] = rn.nextDouble();
			}
		}
		for (int i = 0; i < len; i++) {
			x[i] = IDF.beta(x[i], a, b);
		}
		return x;
	}

	public double[] laplaceDistArray(int len, double beta) {
		double[] p = uniformDistArray(len);
		int i;
		for (i = 0; i < len; i++) {
			if (rn.nextDouble() < 0.5) {
				p[i] = -beta * Math.log(1.0 - p[i]);
			} else {
				p[i] = beta * Math.log(1.0 - p[i]);
			}
		}
		return p;
	}

	public double[] relayDistArray(int len, double sigma) {
		double[] p = uniformDistArray(len);
		int i;
		for (i = 0; i < len; i++) {
			p[i] = sigma * Math.sqrt(-2.0 * Math.log(1.0 - p[i]));
		}
		return p;
	}

	public double[] cauthyDistArray(int len, double alpha, double beta) {
		double[] p = uniformDistArray_OpenInterval(len);
		int i;
		for (i = 0; i < len; i++) {
			p[i] = alpha - beta / Math.tan(Math.PI * p[i]);
		}
		return p;
	}

	public double[] erlangDistArray(int len, int m, double beta) {
		double[] p = new double[len];
		int i, j;
		for (i = 0; i < len; i++) {
			double t = 0.0;
			for (j = 0; j < m; j++) {
				t += Math.log(1.0 - rn.nextDouble());
			}
			p[i] = -beta * t;
		}
		return p;
	}

	public double[] weibullDistArray(int len, double alpha, double beta) {
		double[] p = uniformDistArray(len);
		int i;
		for (i = 0; i < len; i++) {
			p[i] = beta * Math.pow(-Math.log(1.0 - p[i]), 1.0 / alpha);
		}
		return p;
	}

	//==================================
	// Discrete Distribution
	//==================================
	public int[] poisonDistArray(int len, double lambda) {
		int[] p = new int[len];
		double e_lambda = Math.exp(-lambda);
		int i;
		for (i = 0; i < len; i++) {
			double t = 1.0;
			int k = 0;
			while (k < 1000000) {
				t *= rn.nextDouble();
				if (t < e_lambda) {
					p[i] = k;
					break;
				}
				k++;
			}
		}
		return p;
	}
}

class WeightedSampleTree {

	XRandom rnd;
	double[] probs = new double[0];
	int sizeData;
	int sizeLeft;

	public WeightedSampleTree(double[] para, XRandom xr) throws Exception {
		if (para.length <= 1) {
			throw new Exception("The size of para must at least 2");
		}
		rnd = xr;
		sizeData = para.length;
		sizeLeft = sizeData;
		probs = new double[2 * sizeData - 1];
		for (int i = 0; i < sizeData; i++) {
			if (probs[i] < 0) {
				throw new Exception("The item in para must not be negative!");
			}
			probs[sizeData - 1 + i] = para[i];
		}
		System.arraycopy(para, 0, probs, sizeData - 1, sizeData);
		for (int i = sizeData - 2; i >= 0; i--) {
			probs[i] = probs[2 * i + 1] + probs[2 * i + 2];
		}
	}

	public WeightedSampleTree(double[] para) throws Exception {
		if (para.length <= 1) {
			throw new Exception("The size of para must at least 2");
		}
		rnd = new XRandom();
		sizeData = para.length;
		sizeLeft = sizeData;
		probs = new double[2 * sizeData - 1];
		for (int i = 0; i < sizeData; i++) {
			if (probs[i] < 0) {
				throw new Exception("The item in para must not be negative!");
			}
			probs[sizeData - 1 + i] = para[i];
		}
		System.arraycopy(para, 0, probs, sizeData - 1, sizeData);
		for (int i = sizeData - 2; i >= 0; i--) {
			probs[i] = probs[2 * i + 1] + probs[2 * i + 2];
		}
	}
	//get RandomNumber where the index begins from 0

	//    public int getRandomNumber(boolean replace) throws Exception {
	//        if (sizeLeft == 0) {
	//            throw new Exception("No data left");
	//        }
	//        int i = 0;
	//        sizeLeft--;
	//        while (i < sizeData - 1) {
	//            if (rnd.bernoulli(probs[2 * i + 1] / probs[i])) {
	//                i = 2 * i + 1;
	//            } else {
	//                i = 2 * i + 2;
	//            }
	//        }
	//        if (!replace) {
	//            remove(i);
	//        }
	//        return i - sizeData + 1;
	//    }
	public int getRandomNumber(boolean replace) throws Exception {
		if (sizeLeft == 0) {
			throw new Exception("No data left");
		}
		int i = 0;
		while (i < sizeData - 1) {
			if (rnd.bernoulli(probs[2 * i + 1] / probs[i])) {
				i = 2 * i + 1;
			} else {
				i = 2 * i + 2;
			}
		}
		if (!replace) {
			remove(i);
			sizeLeft--;
		}
		return i - sizeData + 1;
	}

	private void remove(int i) {
		double p = probs[i];
		while (i > 0) {
			probs[i] -= p;
			i = (i - 1) / 2;
		}
		probs[0] -= p;
	}
}
