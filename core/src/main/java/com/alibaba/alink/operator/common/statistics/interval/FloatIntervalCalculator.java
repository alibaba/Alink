package com.alibaba.alink.operator.common.statistics.interval;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.common.statistics.statistics.IntervalMeasureCalculator;

import java.math.BigDecimal;

/**
 * @author yangxu
 */
public class FloatIntervalCalculator<T extends Number> extends BaseIntervalCalculator {

	public FloatIntervalCalculator(double start, double step, long[] count) {
		this(start, step, count, DefaultMagnitude);
	}

	public FloatIntervalCalculator(double start, double step, long[] count, int magnitude) {
		this(start, step, count, null, magnitude);
	}

	public FloatIntervalCalculator(double start, double step, long[] count, IntervalMeasureCalculator[][] mcs, int magnitude) {
		this(Math.round(start / step), Math.round(Math.log10(step)) - 1000, count, mcs, magnitude);
	}

	public FloatIntervalCalculator(long start, long step, long[] count, int magnitude) {
		this(start, step, count, null, magnitude);
	}

	public FloatIntervalCalculator(long start, long step, long[] count, IntervalMeasureCalculator[][] mcs, int magnitude) {
		super(start, step, count, mcs, magnitude);
		updateStepBD();
		this.startIndex = start;
	}

	public void calculate(T val, double[] colvals) {
		calculate(val.doubleValue(), colvals);
	}

	public void calculate(T[] vals, double[][] colvals) {
		double[] ds = new double[vals.length];
		for (int i = 0; i < vals.length; i++) {
			ds[i] = vals[i].doubleValue();
		}
		calculate(ds, colvals);
	}

	///////////////////////////////////////////////////////////////////////////////////
	//  由直方图目标数据和其他需要参加统计的数据，创建IntervalCalculator
	///////////////////////////////////////////////////////////////////////////////////
	//public static BaseIntervalCalculator1 create(long[] vals, int magnitude) {
	//	return create(vals, null, magnitude);
	//}
	//
	//public static BaseIntervalCalculator1 create(long[] vals, double[][] colvals) {
	//	return create(vals, colvals, BaseIntervalCalculator1.DefaultMagnitude);
	//}
	//
	//public static BaseIntervalCalculator1 create(long[] vals, double[][] colvals, int magnitude) {
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
	//	BaseIntervalCalculator1 xi = getEmptyInterval(minVal, maxVal, nCol, magnitude);
	//	xi.calculate(vals, colvals);
	//
	//	return xi;
	//}
	//
	//public static BaseIntervalCalculator1 create(double[] vals, int magnitude) {
	//	return create(vals, null, magnitude);
	//}
	//
	//public static BaseIntervalCalculator1 create(double[] vals, double[][] colvals) {
	//	return create(vals, colvals, BaseIntervalCalculator1.DefaultMagnitude);
	//}
	//
	////public static BaseIntervalCalculator1 create(double[] vals, double[][] colvals, int magnitude) {
	////	if (null == vals || vals.length == 0) {
	////		throw new AkIllegalDataException("");
	////	}
	////
	////	double minVal = vals[0];
	////	double maxVal = vals[0];
	////	for (int i = 0; i < vals.length; i++) {
	////		if (minVal > vals[i]) {
	////			minVal = vals[i];
	////		}
	////		if (maxVal < vals[i]) {
	////			maxVal = vals[i];
	////		}
	////	}
	////	int nCol = -1;
	////	if (null != colvals) {
	////		nCol = colvals[0].length;
	////	}
	////	BaseIntervalCalculator1 xi = getEmptyInterval(minVal, maxVal, nCol, magnitude);
	////	xi.calculate(vals, colvals);
	////
	////	return xi;
	////}

	/**
	 * 区间合并
	 *
	 * @param ia ：参加合并的区间
	 * @param ib ：参加合并的区间
	 * @return 合并后的区间
	 * @throws CloneNotSupportedException
	 */
	public static FloatIntervalCalculator combine(FloatIntervalCalculator ia, FloatIntervalCalculator ib) {
		if (null == ia || null == ib) {
			return null;
		}
		if (ia.magnitude != ib.magnitude) {
			throw new AkIllegalDataException("Two merge XInterval must have same magnitude!");
		}
		FloatIntervalCalculator x = null;
		FloatIntervalCalculator y = null;
		try {
			if (ia.step > ib.step) {
				x = (FloatIntervalCalculator) ia.clone();
				y = (FloatIntervalCalculator) ib.clone();
			} else {
				x = (FloatIntervalCalculator) ib.clone();
				y = (FloatIntervalCalculator) ia.clone();
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

	@Override
	public Object clone() throws CloneNotSupportedException {
		FloatIntervalCalculator sd = (FloatIntervalCalculator) super.clone();
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
	@Override
	public String getDataType() {
		return "Decimal";
	}

	/**
	 * *
	 * 获取左边界值
	 *
	 * @return 左边界值
	 */
	@Override
	BigDecimal getLeftBound() {
		return this.stepBD.multiply(BigDecimal.valueOf(startIndex));
	}

	/**
	 * *
	 * 获取基本步长
	 *
	 * @return 基本步长
	 */
	@Override
	public BigDecimal getStep() {
		return this.stepBD;
	}

	/**
	 * *
	 * 获取指定分界点的值
	 *
	 * @param index 指定分界点的索引
	 * @return 指定分界点的值
	 */
	@Override
	public BigDecimal getTag(long index) {
		return this.stepBD.multiply(BigDecimal.valueOf(startIndex + index));
	}

	///////////////////////////////////////////////////////////////////////////////////
	//  增量计算新增数据
	///////////////////////////////////////////////////////////////////////////////////
	@Override
	void updateStepBD() {
		if (this.step == -1000) {
			stepBD = BigDecimal.ONE;
		} else if (this.step > -1000) {
			stepBD = BigDecimal.TEN.pow((int) (this.step + 1000));
		} else {
			stepBD = new BigDecimal(1).divide(BigDecimal.TEN.pow((int) (0 - 1000 - this.step)));
		}
	}

	@Override
	long getNextScale() {
		if (this.step < 0) {
			if (this.step < -1) {
				return 10;
			}
		}

		throw new AkIllegalDataException("Not support this data type or wrong step!");
	}

	@Override
	void adjustStepByScale(long scale) {
		if (1 < scale) {
			if (this.step < 0) {
				long s = scale;

				while (s > 1) {
					s /= 10;
					this.step++;
					this.updateStepBD();
				}
			} else {
				throw new AkIllegalDataException("Not support this data type or wrong step!");
			}
		}
	}

}
