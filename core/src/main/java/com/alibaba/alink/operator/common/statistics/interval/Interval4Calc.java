package com.alibaba.alink.operator.common.statistics.interval;

import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.operator.common.statistics.statistics.IntervalCalculator;
import com.alibaba.alink.operator.common.statistics.statistics.IntervalMeasureCalculator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 直方图显示区间的计算
 * <p>
 * 支持类型：Double.class, Long.class, Date.class
 * <p>
 * 直方图的数据来源： 1. 经过基本统计计算，能得到频率信息的数据，直接用频率计算 2.
 * 否则，使用基本统计计算出的IntervalCalculator类型结果，其中包含基本细分区间
 * <p>
 * 直方图绘图参数： 1. 绘图区间为半闭半开区间 2.
 * 如果不指定左边界或者右边界，则从频率或IntervalCalculator类型结果中的最小值或最大值求出 3.
 * 可以建议绘图步长或直方图区间个数，若两个参数同时给出，则优先考虑绘图步长
 *
 * @author yangxu
 */
public class Interval4Calc {

	/**
	 * myformat: 定义日期类型格式，用来输出日期类型数据的间隔标签
	 */
	public static final SimpleDateFormat myformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	/**
	 * colType: 数据的类型，取值为三种Double.class, Long.class, Date.class
	 */
	String dataType = null;
	/**
	 * 分块区间的个数
	 */
	int nBlock;
	/**
	 * Long.class, Date.class类型数据的起始位置和步长
	 */
	long startLong;
	long stepLong;
	/**
	 * Double.class类型数据的起始位置和步长,用BigDecimal类型表示
	 */
	BigDecimal startBD = null;
	BigDecimal stepBD = null;
	/**
	 * 如果是对直方图数据进行计算，下面2个变量需要赋值 ic指向原始直方图数据 sizeBlock：显示的单个分块区间对应原始直方图基本区间的个数
	 */
	IntervalCalculator ic = null;
	int sizeBlock = -1;

	/**
	 * 获取直方图显示区间
	 *
	 * @param left            ：显示区域左边界
	 * @param right           ：显示区域右边界
	 * @param preferStep：建议步长
	 * @param preferN         ：建议直方图区间个数
	 * @param ic              ：IntervalCalculator类型数据，其中包含基本细分区间
	 * @return 直方图显示区间
	 */
	public static Interval4Display display(long left, long right, long preferStep, int preferN, IntervalCalculator
		ic) {
		if (null == ic || ic.getDataType().equals("Decimal")) {
			throw new AkIllegalStateException("");
		}
		Interval4Calc itvc = Interval4Calc.calculate(left, right, preferStep, preferN, ic.getDataType(), ic);
		return itvc.toInterval4Display();
	}

	///////////////////////////////////////////
	//  获取直方图显示区间
	///////////////////////////////////////////

	/**
	 * 获取直方图显示区间
	 *
	 * @param left            ：显示区域左边界
	 * @param right           ：显示区域右边界
	 * @param preferStep：建议步长
	 * @param preferN         ：建议直方图区间个数
	 * @param ic              ：IntervalCalculator类型数据，其中包含基本细分区间
	 * @return 直方图显示区间
	 */
	public static Interval4Display display(String left, String right, String preferStep, int preferN,
										   IntervalCalculator ic) {
		if (null == ic) {
			throw new AkIllegalStateException("");
		}
		if (ic.getDataType().equals("Decimal")) {
			Interval4Calc itvc = Interval4Calc.calculate(left, right, preferStep, preferN, ic);
			return itvc.toInterval4Display();
		} else {
			long iLeft, iRight, iPreferStep;
			if (null == left) {
				iLeft = ic.getLeftBound().longValue();
			} else {
				iLeft = Long.parseLong(left);
			}
			if (null == right) {
				iRight = ic.getTag(ic.n).longValue();
			} else {
				iRight = Long.parseLong(right);
			}
			if (null == preferStep) {
				iPreferStep = -1;
			} else {
				iPreferStep = Long.parseLong(preferStep);
			}
			Interval4Calc itvc = Interval4Calc.calculate(iLeft, iRight, iPreferStep, preferN, ic.getDataType(), ic);
			return itvc.toInterval4Display();
		}
	}

	/**
	 * 获取直方图显示区间
	 *
	 * @param left            ：显示区域左边界
	 * @param right           ：显示区域右边界
	 * @param preferStep：建议步长
	 * @param preferN         ：建议直方图区间个数
	 * @param dataType        ：数据类型
	 * @param items           ：数据频率中的元素项
	 * @param vals            ：数据频率中的频数项
	 * @return 直方图显示区间
	 */
	public static Interval4Display display(long left, long right, long preferStep, int preferN, String dataType,
										   long[] items, long[] vals) {
		Interval4Calc itvc = Interval4Calc.calculate(left, right, preferStep, preferN, dataType, null);
		return itvc.toInterval4Display(items, vals, dataType);
	}

	/**
	 * 获取直方图显示区间
	 *
	 * @param left            ：显示区域左边界
	 * @param right           ：显示区域右边界
	 * @param preferStep：建议步长
	 * @param preferN         ：建议直方图区间个数
	 * @param dataType        ：数据类型
	 * @param items           ：数据频率中的元素项
	 * @param vals            ：数据频率中的频数项
	 * @return 直方图显示区间
	 */
	public static Interval4Display display(String left, String right, String preferStep, int preferN, String dataType,
										   long[] items, long[] vals) {
		long iLeft, iRight, iPreferStep;
		if (null == left) {
			iLeft = Min(items);//items[0];
		} else {
			iLeft = Long.parseLong(left);
		}

		if (null == right) {
			iRight = Max(items) + 1;//items[items.size - 1];
		} else {
			iRight = Long.parseLong(right);
		}

		if (null == preferStep) {
			iPreferStep = -1;
		} else {
			iPreferStep = Long.parseLong(preferStep);
		}

		return display(iLeft, iRight, iPreferStep, preferN, dataType, items, vals);
	}

	/**
	 * 获取直方图显示区间
	 *
	 * @param left            ：显示区域左边界
	 * @param right           ：显示区域右边界
	 * @param preferStep：建议步长
	 * @param preferN         ：建议直方图区间个数
	 * @param items           ：数据频率中的元素项
	 * @param vals            ：数据频率中的频数项
	 * @return 直方图显示区间
	 */
	public static Interval4Display display(String left, String right, String preferStep, int preferN, double[] items,
										   long[] vals) {
		String iLeft, iRight;
		if (null == left) {
			iLeft = Double.toString(Min(items));
		} else {
			iLeft = left;
		}
		if (null == right) {
			double tmin = Min(items);
			double tmax = Max(items);
			double te = 1;
			if ((tmax - tmin) / 10000 < te) {
				te = (tmax - tmin) / 10000;
			}
			iRight = Double.toString(Max(items) + te);
		} else {
			iRight = right;
		}

		Interval4Calc itvc = Interval4Calc.calculate(iLeft, iRight, preferStep, preferN, null);
		return itvc.toInterval4Display(items, vals);
	}

	///////////////////////////////////////////
	//  计算直方图显示区间的划分方案
	///////////////////////////////////////////
	private static long getNextStepLong(long step, boolean isDateType) {
		if (isDateType) {
			for (int i = 0; i < IntervalCalculator.constSteps4DateType.length - 1; i++) {
				if (IntervalCalculator.constSteps4DateType[i] > step) {
					return IntervalCalculator.constSteps4DateType[i];
				}
			}
			if (step * 10 / 10 == step) {
				return step * 10;
			} else {
				throw new AkIllegalStateException("");
			}
		} else {
			return step * 10;
		}
	}

	/**
	 * 计算直方图显示区间的划分方案
	 *
	 * @param left            ：显示区域左边界
	 * @param right           ：显示区域右边界
	 * @param preferStep：建议步长
	 * @param preferN         ：建议直方图区间个数
	 * @param dataType        ：数据类型
	 * @param ic              ：IntervalCalculator类型数据，其中包含基本细分区间
	 * @return 直方图显示区间的划分方案
	 */
	static Interval4Calc calculate(long left, long right, long preferStep, int preferN, String dataType,
								   IntervalCalculator ic) {
		//if (colType.equals("Decimal") || colType.equals("Integer")) {
		if (dataType.equals("Decimal")) {
			throw new AkIllegalStateException("");
		}
		if (null != ic && ic.getDataType() != dataType) {
			throw new AkIllegalStateException("");
		}

		long icstep = 1;
		if (null != ic) {
			icstep = ic.getStep().longValue();
		}
		long step = -1;
		int sizeBlock = -1;

		//根据用户是否给出建议步长，分情况分析
		if (preferStep > 0) {
			step = icstep;
			boolean isDateType = (dataType.equals("Date"));
			if (step < preferStep) {
				long nextstep = getNextStepLong(step, isDateType);
				while (nextstep < preferStep) {
					step = nextstep;
					nextstep = getNextStepLong(step, isDateType);
				}
			}
			sizeBlock = Math.max(1, (int) (preferStep / step));
		} else {
			if (preferN < 1) {
				throw new AkIllegalStateException("");
			}
			step = icstep;
			boolean isDateType = (dataType.equals("Date"));
			if ((right - left) / step > preferN) {
				long nextstep = getNextStepLong(step, isDateType);
				while ((right - left) / nextstep > preferN) {
					step = nextstep;
					nextstep = getNextStepLong(step, isDateType);
				}
			}
			sizeBlock = Math.max(1, (int) ((right - left) / step / preferN));
		}

		long scale = step / icstep;
		long offset = IntervalCalculator.calcIntervalVal(left, step);
		long end = IntervalCalculator.calcIntervalVal(right - 1, step);
		int n = (int) ((end - offset + 1 + sizeBlock - 1) / sizeBlock);

		Interval4Calc r = new Interval4Calc();
		r.nBlock = n;
		r.startLong = offset * step;
		r.stepLong = step * sizeBlock;
		r.dataType = dataType;
		if (null != ic) {
			r.ic = ic;
			r.sizeBlock = sizeBlock * (int) scale;
		}
		return r;
	}

	/**
	 * 计算直方图显示区间的划分方案
	 *
	 * @param left            ：显示区域左边界
	 * @param right           ：显示区域右边界
	 * @param preferStep：建议步长
	 * @param preferN         ：建议直方图区间个数
	 * @param ic              ：IntervalCalculator类型数据，其中包含基本细分区间
	 * @return 直方图显示区间的划分方案
	 */
	static Interval4Calc calculate(String left, String right, String preferStep, int preferN, IntervalCalculator ic) {
		if (null != ic) {
			if (null == left) {
				left = ic.getLeftBound().toString();
			}
			if (null == right) {
				right = ic.getTag(ic.n).toString();
			}
		}
		if (null == left || null == right || Double.parseDouble(left) >= Double.parseDouble(right)) {
			throw new AkIllegalStateException("");
		}

		BigDecimal stepBD = new BigDecimal(1);
		double min = Double.parseDouble(left);
		double max = Double.parseDouble(right);
		if (null != ic) {
			stepBD = ic.getStep();
		} else {
			int k = -300; //double类型的最小精度
			if (0 != min || 0 != max) {
				int k1 = (int) Math.log10(Math.abs(min) + Math.abs(max));
				k = Math.max(k, k1 - 19);//long型数据大约19个有效数字

				if (min != max) {
					int k2 = (int) (Math.log10(max - min) - Math.log10(preferN));
					k = Math.max(k, k2);
				}
			}
			if (k > 1) {
				stepBD = BigDecimal.TEN.pow(k - 1);
			} else if (k <= 0) {
				stepBD = new BigDecimal(1).divide(BigDecimal.TEN.pow(1 - k));
			}
		}
		long valLeft, valRight;
		valLeft = IntervalCalculator.calcIntervalVal(min, stepBD);
		valRight = IntervalCalculator.calcIntervalVal(max, stepBD);
		int sizeBlock;
		long scale;
		if (null == preferStep) {
			if (preferN < 1) {
				throw new AkIllegalStateException("");
			}
			scale = 1;
			if ((valRight - valLeft) / scale > preferN) {
				long nextscale = scale * 10;
				while ((valRight - valLeft) / nextscale > preferN) {
					scale = nextscale;
					nextscale *= 10;
				}
			}
			sizeBlock = Math.max(1, (int) Math.floor((valRight - valLeft) / scale / preferN));
		} else {
			scale = 1;
			BigDecimal preferStepBD = new BigDecimal(preferStep);
			if (stepBD.multiply(BigDecimal.valueOf(scale)).subtract(preferStepBD).signum() < 0) {
				long nextscale = scale * 10;
				while (stepBD.multiply(BigDecimal.valueOf(nextscale)).subtract(preferStepBD).signum() < 0) {
					scale = nextscale;
					nextscale *= 10;
				}
			}
			sizeBlock = Math.max(1,
				preferStepBD.divide(stepBD.multiply(BigDecimal.valueOf(scale)), 2, RoundingMode.FLOOR).intValue());
		}
		stepBD = stepBD.multiply(BigDecimal.valueOf(scale));

		long offset = IntervalCalculator.calcIntervalVal(new BigDecimal(left), stepBD);
		long end = IntervalCalculator.calcIntervalVal(new BigDecimal(right), stepBD);
		if (stepBD.multiply(BigDecimal.valueOf(end)).subtract(new BigDecimal(right)).signum() == 0) {
			end -= 1;
		}
		int n = (int) ((end - offset + 1 + sizeBlock - 1) / sizeBlock);
		Interval4Calc r = new Interval4Calc();
		r.nBlock = n;
		r.startBD = BigDecimal.valueOf(offset).multiply(stepBD);
		r.stepBD = stepBD.multiply(BigDecimal.valueOf(sizeBlock));
		r.dataType = "Decimal";
		if (null != ic) {
			r.sizeBlock = sizeBlock * (int) scale;
			r.ic = ic;
		}
		return r;
	}

	@Override
	public String toString() {
		return "Interval2{" + "nBlock=" + nBlock + ", startLong=" + startLong + ", stepLong=" + stepLong + ", startBD="
			+ startBD + ", stepBD=" + stepBD + ", colType=" + dataType + ", ic=" + (ic != null) + ", sizeBlock="
			+ sizeBlock + '}';
	}

	/////////////////////////////////////////////////////////////////
	//  由直方图显示区间的划分方案 + 频率数据或IntervalCalculator类型
	//  数据（其中包含基本细分区间），计算直方图显示区间的数据
	/////////////////////////////////////////////////////////////////

	/**
	 * @param items ：数据频率中的元素项
	 * @param vals  ：数据频率中的频数项
	 * @return
	 */
	Interval4Display toInterval4Display(double[] items, long[] vals) {
		Interval4Display
			dis = new Interval4Display();
		int n = this.nBlock;
		dis.n = n;
		dis.step = this.stepBD.stripTrailingZeros().toPlainString();
		dis.count = new long[n];
		dis.tags = new String[n + 1];
		for (int i = 0; i <= n; i++) {
			dis.tags[i] = this.stepBD.multiply(BigDecimal.valueOf(i)).add(this.startBD).stripTrailingZeros()
				.toPlainString();
		}
		double left = this.startBD.doubleValue();
		double right = this.stepBD.multiply(BigDecimal.valueOf(n)).add(this.startBD).doubleValue();
		for (int i = 0; i < items.length; i++) {
			if (items[i] >= left && items[i] < right) {
				long idx = IntervalCalculator.calcIntervalVal(BigDecimal.valueOf(items[i]).subtract(this.startBD),
					this.stepBD);
				if (idx >= 0 && idx < n) {
					dis.count[(int) idx] += vals[i];
				}
			}
		}
		return dis;
	}

	/**
	 * @param items    ：数据频率中的元素项
	 * @param vals     ：数据频率中的频数项
	 * @param dataType ：数据类型
	 * @return
	 */
	Interval4Display toInterval4Display(long[] items, long[] vals, String dataType) {
		if (!this.dataType.equals(dataType)) {
			throw new AkIllegalStateException("");
		}
		Interval4Display
			dis = new Interval4Display();
		int n = this.nBlock;
		dis.n = n;
		dis.step = Long.toString(stepLong);
		dis.count = new long[n];
		dis.tags = new String[n + 1];
		for (int i = 0; i <= n; i++) {
			if (this.dataType.equals("Date")) {
				dis.tags[i] = myformat.format(new Date(this.startLong + i * this.stepLong));
			} else if (this.dataType.equals("Integer")) {
				dis.tags[i] = String.valueOf(this.startLong + i * this.stepLong);
			} else {
				throw new AkIllegalStateException("");
			}
		}
		for (int i = 0; i < items.length; i++) {
			long idx = IntervalCalculator.calcIntervalVal(items[i] - startLong, stepLong);
			if (idx >= 0 && idx < n) {
				dis.count[(int) idx] += vals[i];
			}
		}
		return dis;
	}

	Interval4Display toInterval4Display() {
		if (null == this.ic) {
			throw new AkIllegalStateException("");
		}
		Interval4Display dis = new Interval4Display();
		int n = this.nBlock;
		dis.n = n;
		if (this.dataType.equals("Decimal")) {
			dis.step = this.stepBD.stripTrailingZeros().toPlainString();
		} else {
			dis.step = Long.toString(this.stepLong);
		}
		dis.count = new long[n];
		dis.tags = new String[n + 1];
		if (ic.nCol > 0) {
			dis.nCol = ic.nCol;
			dis.mcs = new IntervalMeasureCalculator[n][dis.nCol];
			for (int i = 0; i < n; i++) {
				for (int j = 0; j < dis.nCol; j++) {
					dis.mcs[i][j] = new IntervalMeasureCalculator();
				}
			}
		}
		long offset = -1;
		if (ic.getDataType().equals("Decimal")) {
			offset = IntervalCalculator.calcIntervalVal(this.startBD.subtract(ic.getLeftBound()), ic.getStep());
		} else {
			offset = (this.startLong - ic.getLeftBound().longValue()) / ic.getStep().longValue();
		}
		if (ic.getDataType().equals("Date")) {
			for (int i = 0; i <= n; i++) {
				BigDecimal bd = ic.getTag(offset + i * sizeBlock);
				dis.tags[i] = myformat.format(new Date(bd.longValue()));
			}
		} else {
			for (int i = 0; i <= n; i++) {
				BigDecimal bd = ic.getTag(offset + i * sizeBlock);
				if (bd.compareTo(BigDecimal.ZERO) == 0) {
					dis.tags[i] = "0";
				} else {
					dis.tags[i] = bd.stripTrailingZeros().toPlainString();
				}
			}
		}
		for (int i = 0; i < n; i++) {
			for (long idx = offset + i * sizeBlock; idx < offset + (i + 1) * sizeBlock; idx++) {
				if (idx >= 0 && idx < ic.n) {
					dis.count[i] += ic.count[(int) idx];
					if (null != dis.mcs) {
						for (int j = 0; j < dis.nCol; j++) {
							if (ic.mcs != null && ic.mcs[(int) idx] != null && ic.mcs[(int) idx][j] != null) {
								dis.mcs[i][j].calculate(ic.mcs[(int) idx][j]);
							}
						}
					}
				}
			}
		}

		return dis;
	}

	static long Max(long[] counts) {
		long max = Long.MIN_VALUE;
		for (int i = 0; i < counts.length; i++) {
			if (max < counts[i]) {
				max = counts[i];
			}
		}
		return max;
	}

	static long Min(long[] counts) {
		long min = Long.MAX_VALUE;
		for (long count : counts) {
			if (min > count) {
				min = count;
			}
		}
		return min;
	}

	public static double Max(double[] counts) {
		double max = Double.MIN_VALUE;
		for (double count : counts) {
			if (max < count) {
				max = count;
			}
		}
		return max;
	}

	static double Min(double[] counts) {
		double min = Double.MAX_VALUE;
		for (double count : counts) {
			if (min > count) {
				min = count;
			}
		}
		return min;
	}

}
