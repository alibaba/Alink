package com.alibaba.alink.operator.common.statistics.interval;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.common.statistics.statistics.IntervalMeasureCalculator;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author yangxu
 */
public class DateIntervalCalculator<T extends Date> extends BaseIntervalCalculator {

	public DateIntervalCalculator(long start, long step, long[] count) {
		this(start, step, count, DefaultMagnitude);
	}

	public DateIntervalCalculator(long start, long step, long[] count, int magnitude) {
		this(start, step, count, null, magnitude);
	}

	public DateIntervalCalculator(long start, long step, long[] count, IntervalMeasureCalculator[][] mcs, int magnitude) {
		super(start, step, count, mcs, magnitude);
		this.startIndex = divideInt(start, step);
	}

	public void calculate(T date, double[] colvals) {
		calculate(date.getTime(), colvals);
	}

	public void calculate(T[] dates, double[][] colvals) {
		long[] ds = new long[dates.length];
		for (int i = 0; i < dates.length; i++) {
			ds[i] = dates[i].getTime();
		}
		calculate(ds, colvals);
	}

	///////////////////////////////////////////////////////////////////////////////////
	//  由直方图目标数据的最小值和最大值，创建空的IntervalCalculator
	///////////////////////////////////////////////////////////////////////////////////
	//public static DateIntervalCalculator getEmptyInterval(long min, long max, int nCol) {
	//	return getEmptyInterval(min, max, nCol, DateIntervalCalculator.DefaultMagnitude);
	//}
	//
	//public static DateIntervalCalculator getEmptyInterval(long min, long max, int nCol, int magnitude) {
	//	MeasureCalculator[][] tmpmcs = null;
	//	if (nCol > 0) {
	//		tmpmcs = new MeasureCalculator[1][nCol];
	//		for (int i = 0; i < nCol; i++) {
	//			tmpmcs[0][i] = new MeasureCalculator();
	//		}
	//	}
	//
	//	return new DateIntervalCalculator(Long.class, min, 1, new long[] {0}, tmpmcs, magnitude);
	//
	//}
	//
	//public static DateIntervalCalculator getEmptyInterval(double min, double max, int nCol, int magnitude) {
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
	//		return new DateIntervalCalculator(Double.class, minBD, k - 1 - 1000, new long[] {0}, tmpmcs, magnitude);
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
	public static DateIntervalCalculator combine(DateIntervalCalculator ia, DateIntervalCalculator ib) {
		if (null == ia || null == ib) {
			return null;
		}
		if (ia.magnitude != ib.magnitude) {
			throw new AkIllegalDataException("Two merge XInterval must have same magnitude!");
		}
		DateIntervalCalculator x = null;
		DateIntervalCalculator y = null;
		try {
			if (ia.step > ib.step) {
				x = (DateIntervalCalculator) ia.clone();
				y = (DateIntervalCalculator) ib.clone();
			} else {
				x = (DateIntervalCalculator) ib.clone();
				y = (DateIntervalCalculator) ia.clone();
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
		DateIntervalCalculator sd = (DateIntervalCalculator) super.clone();
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
		return "Date";
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
	public void updateStepBD() {
		this.stepBD = null;
	}

	@Override
	long getNextScale() {
		if (this.step > 0) {
			for (int i = 0; i < constSteps4DateType.length - 1; i++) {
				if (constSteps4DateType[i] == this.step) {
					return constSteps4DateType[i + 1] / constSteps4DateType[i];
				}
			}
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
