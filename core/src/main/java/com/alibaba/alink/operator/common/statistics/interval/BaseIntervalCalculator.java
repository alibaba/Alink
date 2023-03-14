package com.alibaba.alink.operator.common.statistics.interval;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.common.statistics.statistics.IntervalMeasureCalculator;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * @author yangxu
 */
abstract class BaseIntervalCalculator implements Cloneable, Serializable {

	/**
	 * *
	 * 日期类型数据的基本区间长度的可选值数组
	 */
	public static final long[] constSteps4DateType = new long[] {
		1,//1 millisecond
		10L,
		10L * 10,
		10L * 10 * 10, // 1 sec
		10L * 10 * 10 * 10,
		10L * 10 * 10 * 10 * 6,//1 min
		10L * 10 * 10 * 10 * 6 * 10,
		10L * 10 * 10 * 10 * 6 * 10 * 3,//half an hour
		10L * 10 * 10 * 10 * 6 * 10 * 3 * 2,//1 hour
		10L * 10 * 10 * 10 * 6 * 10 * 3 * 2 * 6,
		10L * 10 * 10 * 10 * 6 * 10 * 3 * 2 * 6 * 2,//half a day
		10L * 10 * 10 * 10 * 6 * 10 * 3 * 2 * 6 * 2 * 2,//1 day
		10L * 10 * 10 * 10 * 6 * 10 * 3 * 2 * 6 * 2 * 2 * 10,//10 day
		10L * 10 * 10 * 10 * 6 * 10 * 3 * 2 * 6 * 2 * 2 * 100,//100 day
		10L * 10 * 10 * 10 * 6 * 10 * 3 * 2 * 6 * 2 * 2 * 1000,//1000 day
		10L * 10 * 10 * 10 * 6 * 10 * 3 * 2 * 6 * 2 * 2 * 10000,//10000 day
		10L * 10 * 10 * 10 * 6 * 10 * 3 * 2 * 6 * 2 * 2 * 100000,//100000 day
	};

	protected final static int DefaultMagnitude = 1000;

	public int n;
	public long[] count = null;
	public int nCol = -1;

	/**
	 * *
	 * 每个基本区间内数据的基本统计计算量
	 */
	public IntervalMeasureCalculator[][] mcs = null;
	public int magnitude; // magnitude < n <= 10 * magnitude
	public long startIndex;

	/**
	 * *
	 * step positive: for Long and Date type step negative: for Double type
	 * 10^(step+1000) is the real step value
	 */
	public long step;
	public BigDecimal stepBD = null;

	///////////////////////////////////////////////////////////////////////////////////
	//  IntervalCalculator 构造函数
	///////////////////////////////////////////////////////////////////////////////////

	BaseIntervalCalculator(long start, long step, long[] count, IntervalMeasureCalculator[][] mcs, int magnitude) {
		if ((10 * (long) magnitude > Integer.MAX_VALUE) || (magnitude < 1)) {
			throw new AkIllegalArgumentException("");
		} else {
			this.magnitude = magnitude;
		}
		this.step = step;
		this.n = count.length;
		this.count = count;
		if (null != mcs) {
			this.nCol = mcs[0].length;
			this.mcs = mcs;
		}
	}

	protected static long divideInt(long k, long m) {
		if (k >= 0) {
			return k / m;
		} else {
			return (k - m + 1) / m;
		}
	}

	public static long calcIntervalVal(long val, long curStep) {
		return divideInt(val, curStep);
	}

	///////////////////////////////////////////////////////////////////////////////////
	//  获取区间数据
	///////////////////////////////////////////////////////////////////////////////////

	public static long calcIntervalVal(double val, BigDecimal curStep) {
		return calcIntervalVal(new BigDecimal(val), curStep);
	}

	public static long calcIntervalVal(BigDecimal val, BigDecimal curStep) {
		BigInteger k = calcIntervalValBD(val, curStep);
		if (BigInteger.valueOf(k.longValue()).subtract(k).signum() == 0) {
			return k.longValue();
		} else {
			throw new AkIllegalArgumentException("");
		}
	}

	private static BigInteger calcIntervalValBD(BigDecimal valBD, BigDecimal curStep) {
		//        return valBD.divide(curStep, 2, RoundingMode.FLOOR).toBigInteger();
		BigInteger bd = valBD.divide(curStep, 2, RoundingMode.FLOOR).toBigInteger();
		if (valBD.subtract(curStep.multiply(new BigDecimal(bd))).signum() < 0) {
			return bd.subtract(BigInteger.ONE);
		} else {
			return bd;
		}
	}

	private static BigInteger calcIntervalValBD(double val, BigDecimal curStep) {
		return calcIntervalValBD(new BigDecimal(val), curStep);
	}

	///////////////////////////////////////////////////////////////////////////////////
	//  由直方图目标数据和其他需要参加统计的数据，创建IntervalCalculator
	///////////////////////////////////////////////////////////////////////////////////
	//public static BaseIntervalCalculator create(long[] vals, int magnitude) {
	//	return create(vals, null, magnitude);
	//}
	//
	//public static BaseIntervalCalculator create(long[] vals, double[][] colvals) {
	//	return create(vals, colvals, BaseIntervalCalculator.DefaultMagnitude);
	//}
	//
	//public static BaseIntervalCalculator create(long[] vals, double[][] colvals, int magnitude) {
	//	if (null == vals || vals.length == 0) {
	//		throw new AkIllegalDataException("");
	//	}
	//
	//	long minVal = vals[0];
	//	long maxVal = vals[0];
	//	for (int i = 0; i < vals.length; i++) {
	//		if (minVal > vals[i]) {
	//			minVal = vals[i];
	//		}
	//		if (maxVal < vals[i]) {
	//			maxVal = vals[i];
	//		}
	//	}
	//	int nCol = -1;
	//	if (null != colvals) {
	//		nCol = colvals[0].length;
	//	}
	//	BaseIntervalCalculator xi = getEmptyInterval(minVal, maxVal, nCol, magnitude);
	//	xi.calculate(vals, colvals);
	//
	//	return xi;
	//}
	//
	//public static BaseIntervalCalculator create(double[] vals, int magnitude) {
	//	return create(vals, null, magnitude);
	//}
	//
	//public static BaseIntervalCalculator create(double[] vals, double[][] colvals) {
	//	return create(vals, colvals, BaseIntervalCalculator.DefaultMagnitude);
	//}
	//
	//public static BaseIntervalCalculator create(double[] vals, double[][] colvals, int magnitude) {
	//	if (null == vals || vals.length == 0) {
	//		throw new AkIllegalDataException("");
	//	}
	//
	//	double minVal = vals[0];
	//	double maxVal = vals[0];
	//	for (int i = 0; i < vals.length; i++) {
	//		if (minVal > vals[i]) {
	//			minVal = vals[i];
	//		}
	//		if (maxVal < vals[i]) {
	//			maxVal = vals[i];
	//		}
	//	}
	//	int nCol = -1;
	//	if (null != colvals) {
	//		nCol = colvals[0].length;
	//	}
	//	BaseIntervalCalculator xi = getEmptyInterval(minVal, maxVal, nCol, magnitude);
	//	xi.calculate(vals, colvals);
	//
	//	return xi;
	//}

	///////////////////////////////////////////////////////////////////////////////////
	//  由直方图目标数据的最小值和最大值，创建空的IntervalCalculator
	///////////////////////////////////////////////////////////////////////////////////

	//public static BaseIntervalCalculator getEmptyInterval(long min, long max, int nCol) {
	//	return getEmptyInterval(min, max, nCol, BaseIntervalCalculator.DefaultMagnitude);
	//}
	//
	//public static BaseIntervalCalculator getEmptyInterval(long min, long max, int nCol, int magnitude) {
	//	MeasureCalculator[][] tmpmcs = null;
	//	if (nCol > 0) {
	//		tmpmcs = new MeasureCalculator[1][nCol];
	//		for (int i = 0; i < nCol; i++) {
	//			tmpmcs[0][i] = new MeasureCalculator();
	//		}
	//	}
	//
	//	return new BaseIntervalCalculator(Long.class, min, 1, new long[] {0}, tmpmcs, magnitude);
	//
	//}
	//
	//public static BaseIntervalCalculator getEmptyInterval(double min, double max, int nCol, int magnitude) {
	//	if (Double.NEGATIVE_INFINITY < min && min <= max && max < Double.POSITIVE_INFINITY) {
	//		int k = -300; //double类型的最小精度
	//		if (0 != min || 0 != max) {
	//			int k1 = (int) Math.log10(Math.abs(min) + Math.abs(max));
	//			k = Math.max(k, k1 - 19);//long型数据大约19个有效数字
	//
	//			if (min != max) {
	//				int k2 = (int) (Math.log10(max - min) - Math.log10(magnitude));
	//				k = Math.max(k, k2);
	//			}
	//		}
	//		BigDecimal stepBD = new BigDecimal(1);
	//		if (k > 1) {
	//			stepBD = BigDecimal.TEN.pow(k - 1);
	//		} else if (k <= 0) {
	//			stepBD = new BigDecimal(1).divide(BigDecimal.TEN.pow(1 - k));
	//		}
	//
	//		long minBD = calcIntervalValBD(min, stepBD).longValue();
	//		MeasureCalculator[][] tmpmcs = null;
	//		if (nCol > 0) {
	//			tmpmcs = new MeasureCalculator[1][nCol];
	//			for (int i = 0; i < nCol; i++) {
	//				tmpmcs[0][i] = new MeasureCalculator();
	//			}
	//		}
	//
	//		return new BaseIntervalCalculator(Double.class, minBD, k - 1 - 1000, new long[] {0}, tmpmcs, magnitude);
	//
	//	} else {
	//		throw new AkIllegalDataException("");
	//	}
	//}

	/**
	 * 区间合并
	 *
	 * @param ia ：参加合并的区间
	 * @param ib ：参加合并的区间
	 * @return 合并后的区间
	 * @throws CloneNotSupportedException
	 */
	public static BaseIntervalCalculator combine(BaseIntervalCalculator ia, BaseIntervalCalculator ib) {
		if (null == ia || null == ib) {
			return null;
		}
		if (ia.magnitude != ib.magnitude) {
			throw new AkIllegalDataException("Two merge XInterval must have same magnitude!");
		}
		BaseIntervalCalculator x = null;
		BaseIntervalCalculator y = null;
		try {
			if (ia.step > ib.step) {
				x = (BaseIntervalCalculator) ia.clone();
				y = (BaseIntervalCalculator) ib.clone();
			} else {
				x = (BaseIntervalCalculator) ib.clone();
				y = (BaseIntervalCalculator) ia.clone();
			}
		} catch (Exception ex) {
			throw new AkIllegalDataException(ex.getMessage());
		}

		while (x.step > y.step) {
			y.upgrade();
		}

		long min = Math.min(x.startIndex, y.startIndex);
		long max = Math.max(x.startIndex + x.n - 1, y.startIndex + y.n - 1);

		x.upgrade(min, max);
		y.upgrade(min, max);

		for (int i = 0; i < x.n; i++) {
			x.count[i] += y.count[i];
		}

		if (null != x.mcs && null != y.mcs) {
			for (int i = 0; i < x.n; i++) {
				for (int j = 0; j < x.nCol; j++) {
					if (y.mcs[i][j] == null) {
						x.mcs[i][j] = null;
					} else {
						x.mcs[i][j].calculate(y.mcs[i][j]);
					}
				}
			}
		} else {
			x.mcs = null;
			x.nCol = 0;
		}

		return x;
	}

	/**
	 * 将一组区间的步长统一为其中最大者
	 *
	 * @param ics 区间组
	 * @return
	 */
	public static boolean update2MaxStep(BaseIntervalCalculator[] ics) {
		if (null == ics || ics.length == 0) {
			throw new AkIllegalDataException("");
		}

		long maxstep = ics[0].step;
		for (int i = 1; i < ics.length; i++) {
			if (maxstep < ics[i].step) {
				maxstep = ics[i].step;
			}
		}

		for (int i = 0; i < ics.length; i++) {
			while (maxstep > ics[i].step) {
				ics[i].upgrade();
			}
		}

		return true;
	}

	/**
	 * 将一组区间的步长统一为其中最大者，并将表示的区间范围统一
	 *
	 * @param ics 区间组
	 * @return
	 */
	public static boolean update2MaxStepSameRange(BaseIntervalCalculator[] ics) {

		update2MaxStep(ics);

		long min = ics[0].startIndex;
		long max = ics[0].startIndex + ics[0].n - 1;
		for (int i = 1; i < ics.length; i++) {
			min = Math.min(min, ics[i].startIndex);
			max = Math.max(max, ics[i].startIndex + ics[i].n - 1);
		}

		for (int i = 0; i < ics.length; i++) {
			ics[i].upgrade(min, max);
		}

		return true;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		BaseIntervalCalculator sd = (BaseIntervalCalculator) super.clone();
		sd.count = this.count.clone();
		if (null != this.mcs) {
			sd.mcs = this.mcs.clone();
		}
		return sd;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();
		sbd.append("startIndex=" + startIndex + ", step=" + step + ", n=" + n + ", magnitude=" + magnitude + '\n');
		for (int i = 0; i < n; i++) {
			sbd.append("count[" + i + "] = " + count[i] + "\n");
		}
		return sbd.toString();
	}

	/**
	 * *
	 * 获取数据类型
	 *
	 * @return 数据类型
	 */
	abstract String getDataType();

	/**
	 * *
	 * 获取左边界值
	 *
	 * @return 左边界值
	 */
	abstract BigDecimal getLeftBound();

	/**
	 * *
	 * 获取基本步长
	 *
	 * @return 基本步长
	 */
	abstract BigDecimal getStep();

	/**
	 * *
	 * 获取指定分界点的值
	 *
	 * @param index 指定分界点的索引
	 * @return 指定分界点的值
	 */
	abstract BigDecimal getTag(long index);

	/**
	 * *
	 * 获取每个基本区间内数据的个数
	 *
	 * @return 每个基本区间内数据的个数
	 */
	public long[] getCount() {
		return this.count.clone();
	}

	public IntervalMeasureCalculator[] updateMeasureCalculatorsByCol(int idx) throws Exception {
		IntervalMeasureCalculator[] measureCalculators = new IntervalMeasureCalculator[n];
		for (int i = 0; i < this.mcs.length; i++) {
			measureCalculators[i] = mcs[i][idx];
		}
		return measureCalculators;

	}

	///////////////////////////////////////////////////////////////////////////////////
	//  增量计算新增数据
	///////////////////////////////////////////////////////////////////////////////////
	abstract void updateStepBD();

	    public void calculate(long val) {
	        calculate(new long[]{val}, null);
	    }

	//    public void calculate(long[] vals) {
	//        calculate(vals, null);
	//    }
	//
	    public void calculate(long val, double[] colvals) {
	        calculate(new long[]{val}, new double[][]{colvals});
	    }

	public void calculate(long[] vals, double[][] colvals) {
		if (null == vals || vals.length == 0) {
			return;
		}

		long minVal = vals[0];
		long maxVal = vals[0];
		for (int i = 0; i < vals.length; i++) {
			if (minVal > vals[i]) {
				minVal = vals[i];
			}
			if (maxVal < vals[i]) {
				maxVal = vals[i];
			}
		}

		long min = toIntervalVal(minVal);
		long max = toIntervalVal(maxVal);
		if ((min < startIndex) || (max >= startIndex + n)) {
			// min 或者 max 不在区间内，需要重新设计区间分划
			min = Math.min(min, startIndex);
			max = Math.max(max, startIndex + n - 1);
			upgrade(min, max);
		}

		for (int i = 0; i < vals.length; i++) {
			int t = (int) (toIntervalVal(vals[i]) - startIndex);
			count[t]++;
			if (null != this.mcs && null != colvals) {
				for (int j = 0; j < this.nCol; j++) {
					mcs[t][j].calculate(colvals[i][j]);
				}
			}
		}
		return;
	}

	//
	//    public void calculate(Date date) {
	//        calculate(new Date[]{date}, null);
	//    }
	//
	//    public void calculate(Date[] dates) {
	//        calculate(dates, null);
	//    }
	//
	//    public void calculate(Date date, double[] colvals) {
	//        calculate(date.getTime(), colvals);
	//    }
	//
	//    public void calculate(Date[] dates, double[][] colvals) {
	//        long[] ds = new long[dates.length];
	//        for (int i = 0; i < dates.length; i++) {
	//            ds[i] = dates[i].getTime();
	//        }
	//        calculate(ds, colvals);
	//    }
	//
	//    public void calculate(double val) {
	//        calculate(new double[]{val}, null);
	//    }
	//
	//    public void calculate(double[] vals) {
	//        calculate(vals, null);
	//    }
	//
	    public void calculate(double val, double[] colvals) {
	        calculate(new double[]{val}, new double[][]{colvals});
	    }

	public void calculate(double[] vals, double[][] colvals) {
		if (null == vals || vals.length == 0) {
			return;
		}

		double minVal = vals[0];
		double maxVal = vals[0];
		for (int i = 0; i < vals.length; i++) {
			if (minVal > vals[i]) {
				minVal = vals[i];
			}
			if (maxVal < vals[i]) {
				maxVal = vals[i];
			}
		}

		while (!hasValidIntervalVal(minVal)) {
			upgrade();
		}
		while (!hasValidIntervalVal(maxVal)) {
			upgrade();
		}

		long min = toIntervalVal(minVal);
		long max = toIntervalVal(maxVal);
		if ((min < startIndex) || (max >= startIndex + n)) {
			// val 不在区间内，需要重新设计区间分划
			min = Math.min(min, startIndex);
			max = Math.max(max, startIndex + n - 1);

			upgrade(min, max);
		}

		for (int i = 0; i < vals.length; i++) {
			int t = (int) (toIntervalVal(vals[i]) - startIndex);
			count[t]++;
			if (null != this.mcs && null != colvals) {
				for (int j = 0; j < this.nCol; j++) {
					mcs[t][j].calculate(colvals[i][j]);
				}
			}
		}
		return;

	}

	abstract long getNextScale();

	protected void upgrade() {
		long scale = getNextScale();
		long startNew = divideInt(startIndex, scale);
		long endNew = divideInt(startIndex + n - 1, scale) + 1;
		subUpgrade(scale, startNew, endNew);
	}

	protected void upgrade(long min, long max) {
		long scale = getScale4Upgrade(min, max);
		long startNew = divideInt(min, scale);
		long endNew = divideInt(max, scale) + 1;
		subUpgrade(scale, startNew, endNew);
	}

	abstract void adjustStepByScale(long scale);

	private void subUpgrade(long scale, long startNew, long endNew) {
		adjustStepByScale(scale);

		int nNew = (int) (endNew - startNew);
		long[] countNew = new long[nNew];
		for (int i = 0; i < n; i++) {
			int t = (int) (divideInt(i + startIndex, scale) - startNew);
			countNew[t] += count[i];
		}

		if (null != this.mcs) {
			IntervalMeasureCalculator[][] mscNew = new IntervalMeasureCalculator[nNew][this.nCol];
			for (int i = 0; i < nNew; i++) {
				for (int j = 0; j < this.nCol; j++) {
					mscNew[i][j] = new IntervalMeasureCalculator();
				}
			}
			for (int i = 0; i < n; i++) {
				int t = (int) (divideInt(i + startIndex, scale) - startNew);
				for (int j = 0; j < nCol; j++) {
					if (this.mcs[i][j] == null) {
						mscNew[t][j] = null;
					} else {
						mscNew[t][j].calculate(this.mcs[i][j]);
					}
				}
			}
			this.mcs = mscNew;
		}

		this.startIndex = startNew;
		this.n = nNew;
		this.count = countNew;
	}

	private long getScale4Upgrade(long min, long max) {
		if (min > max) {
			throw new AkIllegalDataException("");
		}

		long s = 1;
		for (int i = 0; i < 20; i++) {
			long k = divideInt(max + s - 1, s) - divideInt(min, s);
			if (k <= this.magnitude * 10) {
				break;
			} else {
				s *= getNextScale();
			}
		}
		return s;
	}

	private boolean hasValidIntervalVal(double val) {
		BigInteger k = calcIntervalValBD(val, this.stepBD);
		return BigInteger.valueOf(k.longValue()).subtract(k).signum() == 0;
	}

	private long toIntervalVal(long val) {
		return calcIntervalVal(val, this.step);
	}

	private long toIntervalVal(double val) {
		BigInteger k = calcIntervalValBD(val, this.stepBD);
		if (BigInteger.valueOf(k.longValue()).subtract(k).signum() != 0) {
			//有精度损失
			throw new AkIllegalDataException("");
		}
		return k.longValue();
	}
}
