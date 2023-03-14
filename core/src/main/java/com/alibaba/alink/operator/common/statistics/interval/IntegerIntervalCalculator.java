package com.alibaba.alink.operator.common.statistics.interval;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.common.statistics.statistics.IntervalMeasureCalculator;

import java.math.BigDecimal;

/**
 * @author yangxu
 */
public class IntegerIntervalCalculator<T extends Number> extends BaseIntervalCalculator {

	public IntegerIntervalCalculator(long start, long step, long[] count) {
		this(start, step, count, DefaultMagnitude);
	}

	public IntegerIntervalCalculator(long start, long step, long[] count, int magnitude) {
		this(start, step, count, null, magnitude);
	}

	public IntegerIntervalCalculator(long start, long step, long[] count, IntervalMeasureCalculator[][] mcs, int magnitude) {
		super(start, step, count, mcs, magnitude);
		this.startIndex = divideInt(start, step);
	}

	public void calculate(T val, double[] colvals) {
		calculate(val.longValue(), colvals);
	}

	public void calculate(T[] vals, double[][] colvals) {
		long[] ds = new long[vals.length];
		for (int i = 0; i < vals.length; i++) {
			ds[i] = vals[i].longValue();
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

	///////////////////////////////////////////////////////////////////////////////////
	//  由直方图目标数据的最小值和最大值，创建空的IntervalCalculator
	///////////////////////////////////////////////////////////////////////////////////
	public static IntegerIntervalCalculator getEmptyInterval(long min, long max, int nCol) {
		return getEmptyInterval(min, max, nCol, DefaultMagnitude);
	}

	public static IntegerIntervalCalculator getEmptyInterval(long min, long max, int nCol, int magnitude) {
		IntervalMeasureCalculator[][] tmpmcs = null;
		if (nCol > 0) {
			tmpmcs = new IntervalMeasureCalculator[1][nCol];
			for (int i = 0; i < nCol; i++) {
				tmpmcs[0][i] = new IntervalMeasureCalculator();
			}
		}

		return new IntegerIntervalCalculator(min, 1, new long[] {0}, tmpmcs, magnitude);

	}

	/**
	 * 区间合并
	 *
	 * @param ia ：参加合并的区间
	 * @param ib ：参加合并的区间
	 * @return 合并后的区间
	 * @throws CloneNotSupportedException
	 */
	public static IntegerIntervalCalculator combine(IntegerIntervalCalculator ia, IntegerIntervalCalculator ib) {
		if (null == ia || null == ib) {
			return null;
		}
		if (ia.magnitude != ib.magnitude) {
			throw new AkIllegalDataException("Two merge XInterval must have same magnitude!");
		}
		IntegerIntervalCalculator x = null;
		IntegerIntervalCalculator y = null;
		try {
			if (ia.step > ib.step) {
				x = (IntegerIntervalCalculator) ia.clone();
				y = (IntegerIntervalCalculator) ib.clone();
			} else {
				x = (IntegerIntervalCalculator) ib.clone();
				y = (IntegerIntervalCalculator) ia.clone();
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
		IntegerIntervalCalculator sd = (IntegerIntervalCalculator) super.clone();
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
		return "Integer";
	}

	/**
	 * *
	 * 获取左边界值
	 *
	 * @return 左边界值
	 */
	@Override
	public BigDecimal getLeftBound() {
		return BigDecimal.valueOf(startIndex * step);
	}

	/**
	 * *
	 * 获取基本步长
	 *
	 * @return 基本步长
	 */
	@Override
	public BigDecimal getStep() {
		return BigDecimal.valueOf(this.step);
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
		return BigDecimal.valueOf((startIndex + index) * step);
	}

	///////////////////////////////////////////////////////////////////////////////////
	//  增量计算新增数据
	///////////////////////////////////////////////////////////////////////////////////
	@Override
	void updateStepBD() {
		this.stepBD = null;
	}

	@Override
	long getNextScale() {
		if (this.step > 0) {
			if (this.step * 10 / 10 == this.step) {
				return 10;
			}
		}

		throw new AkIllegalDataException("Not support this data type or wrong step!");
	}

	@Override
	void adjustStepByScale(long scale) {
		if (1 < scale) {
			if (this.step > 0) {
				this.step *= scale;
			} else {
				throw new AkIllegalDataException("Not support this data type or wrong step!");
			}
		}
	}

}
