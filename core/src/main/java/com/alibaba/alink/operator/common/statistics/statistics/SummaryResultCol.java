package com.alibaba.alink.operator.common.statistics.statistics;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.viz.plot.Histogram;
import com.alibaba.alink.common.viz.plot.StackedBar;
import com.alibaba.alink.operator.common.statistics.StatisticUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * @author yangxu
 */
public class SummaryResultCol implements Serializable, Cloneable {

	private static final long serialVersionUID = 4556651871374619811L;
	/**
	 * *
	 * 数据类型
	 */
	public Class dataType;
	/**
	 * *
	 * 全部数据个数，包括缺失值个数，空值个数，正负无穷的数值个数
	 */
	public long countTotal;
	/**
	 * *
	 * 正常计算范围的数据个数，下面的各种统计量都是针对此范围的数据进行计算的
	 */
	public long count;
	/**
	 * *
	 * 缺失值个数，用户没有输入该值
	 */
	public long countMissValue = 0;
	/**
	 * *
	 * 空值个数，用户输入了，但并不是一个数
	 */
	public long countNanValue = 0;
	/**
	 * *
	 * 值为正无穷的数据个数
	 */
	public long countPositiveInfinity = 0;
	/**
	 * *
	 * 值为负无穷的数据个数
	 */
	public long countNegativInfinity = 0;
	/**
	 * *
	 * 数据的和
	 */
	public double sum;
	/**
	 * *
	 * 数据的平方和
	 */
	public double sum2;
	/**
	 * *
	 * 数据的立方和
	 */
	public double sum3;
	/**
	 * *
	 * 数据的四次方和
	 */
	public double sum4;

	public double norm1;

	/**
	 * *
	 * 最小值
	 */
	public Object min;
	/**
	 * *
	 * 最大值
	 */
	public Object max;
	/**
	 * *
	 * 数据TopN
	 */
	public Object[] topItems = null;
	/**
	 * *
	 * 数据BottomN
	 */
	public Object[] bottomItems = null;
	public String colName = null;
	/**
	 * *
	 * 频数信息，若不同数值个数较少时，会计算出此信息, 只对mtable
	 */
	public TreeMap <Object, Long> freq = null;
	/**
	 * *
	 * 基本直方图区间划分及区间内元素个数, 只对mtable
	 */
	public IntervalCalculator itvcalc = null;

	//    Boolean isHasFreq = null;
	//    Boolean isSimpleSumamry = true;
	SummaryResultCol() {
	}

	SummaryResultCol(Class dataType) {
		init(dataType, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
	}

	public SummaryResultCol(SRC src) throws ClassNotFoundException {
		init(Class.forName(src.dataType), src.countTotal, src.count, src.countMissValue, src.countNanValue,
			src.countPositiveInfinity, src.countNegativInfinity,
			src.sum, src.normL1, src.sum2, src.sum3, src.sum4, src.min, src.max);
	}

	@Override
	public SummaryResultCol clone() {
		try {
			SummaryResultCol src = new SummaryResultCol(this.toSRC());
			src.topItems = (this.topItems == null) ? null : this.topItems.clone();
			src.bottomItems = (this.bottomItems == null) ? null : this.bottomItems.clone();
			src.colName = this.colName;
			src.freq = (this.freq == null) ? null : (TreeMap <Object, Long>) this.freq.clone();
			src.itvcalc = (this.itvcalc == null) ? null : (IntervalCalculator) this.itvcalc.clone();
			return src;
		} catch (ClassNotFoundException | CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static SummaryResultCol combine(SummaryResultCol src1, SummaryResultCol src2) {
		if (!src1.dataType.equals(src2.dataType)) {
			throw new RuntimeException("The 2 col data types are not equal!");
		}

		if (0 == src1.countTotal) {
			return src2;
		} else if (0 == src2.countTotal) {
			return src1;
		}

		Object minValue = null;
		Object maxValue = null;
		if (src1.count == 0) {
			minValue = src2.min;
			maxValue = src2.max;
		} else if (src2.count == 0) {
			minValue = src1.min;
			maxValue = src1.max;
		} else {
			if (src1.dataType == Double.class) {
				minValue = (Double) (src1.min) < (Double) (src2.min) ? src1.min : src2.min;
				maxValue = (Double) (src1.max) > (Double) (src2.max) ? src1.max : src2.max;
			} else if (src1.dataType == Float.class) {
				minValue = (Float) (src1.min) < (Float) (src2.min) ? src1.min : src2.min;
				maxValue = (Float) (src1.max) > (Float) (src2.max) ? src1.max : src2.max;
			} else if (src1.dataType == Long.class) {
				minValue = (Long) (src1.min) < (Long) (src2.min) ? src1.min : src2.min;
				maxValue = (Long) (src1.max) > (Long) (src2.max) ? src1.max : src2.max;
			} else if (src1.dataType == Short.class) {
				minValue = (Short) (src1.min) < (Short) (src2.min) ? src1.min : src2.min;
				maxValue = (Short) (src1.max) > (Short) (src2.max) ? src1.max : src2.max;
			} else if (src1.dataType == Byte.class) {
				minValue = (Byte) (src1.min) < (Byte) (src2.min) ? src1.min : src2.min;
				maxValue = (Byte) (src1.max) > (Byte) (src2.max) ? src1.max : src2.max;
			} else if (src1.dataType == Integer.class) {
				minValue = (Integer) (src1.min) < (Integer) (src2.min) ? src1.min : src2.min;
				maxValue = (Integer) (src1.max) > (Integer) (src2.max) ? src1.max : src2.max;
			} else if (src1.dataType == String.class) {

			} else if (src1.dataType == Boolean.class) {
				if ((Boolean) src1.min == false || (Boolean) src2.min == false) {
					minValue = false;
				} else {
					minValue = (Boolean) src1.min;
				}

				if ((Boolean) src1.max == true || (Boolean) src2.max == true) {
					maxValue = true;
				} else {
					maxValue = (Boolean) src1.max;
				}
			} else if (src1.dataType == java.sql.Timestamp.class) {
				minValue = (Long) src1.min < (Long) src2.min ? src1.min : src2.min;
				maxValue = (Long) src1.max > (Long) src2.max ? src1.max : src2.max;
			} else {
				throw new RuntimeException("Not implemented yet! " + src1.dataType);
			}

		}
		SummaryResultCol src = new SummaryResultCol();
		src.init(src1.dataType,
			src1.countTotal + src2.countTotal,
			src1.count + src2.count,
			src1.countMissValue + src2.countMissValue,
			src1.countNanValue + src2.countNanValue,
			src1.countPositiveInfinity + src2.countPositiveInfinity,
			src1.countNegativInfinity + src2.countNegativInfinity,
			src1.sum + src2.sum,
			src1.sum + src2.sum,
			src1.sum2 + src2.sum2,
			src1.sum3 + src2.sum3,
			src1.sum4 + src2.sum4,
			minValue, maxValue);

		//topItems
		if (null != src1.topItems && src1.topItems.length > 0
			&& null != src2.topItems && src2.topItems.length > 0
			&& src.dataType != String.class) {
			int k = Math.min(src1.topItems.length, src2.topItems.length);
			src.topItems = new Object[k];
			for (int i = 0, j = 0; i + j < k; ) {
				boolean b;
				if (Double.class == src.dataType) {
					b = (Double) (src1.topItems[i]) >= (Double) (src2.topItems[j]);
				} else if (Integer.class == src.dataType) {
					b = (Integer) (src1.topItems[i]) >= (Integer) (src2.topItems[j]);
				} else if (Long.class == src.dataType || java.sql.Timestamp.class == src.dataType) {
					b = (Long) (src1.topItems[i]) >= (Long) (src2.topItems[j]);
				} else if (Float.class == src.dataType) {
					b = (Float) (src1.topItems[i]) >= (Float) (src2.topItems[j]);
				} else if (Boolean.class == src.dataType) {
					b = convertBoolean((Boolean) (src1.topItems[i])) >= convertBoolean((Boolean) (src2.topItems[j]));
				} else {
					throw new RuntimeException("Not support yet!" + src.dataType);
				}
				if (b) {
					src.topItems[i + j] = src1.topItems[i];
					i++;
				} else {
					src.topItems[i + j] = src2.topItems[j];
					j++;
				}
			}
		}
		//bottomItems
		if (null != src1.bottomItems && src1.bottomItems.length > 0
			&& null != src2.bottomItems && src2.bottomItems.length > 0
			&& String.class != src.dataType) {
			int k = Math.min(src1.bottomItems.length, src2.bottomItems.length);
			src.bottomItems = new Object[k];
			for (int i = 0, j = 0; i + j < k; ) {
				boolean b;
				if (Double.class == src.dataType) {
					b = (Double) (src1.bottomItems[i]) <= (Double) (src2.bottomItems[j]);
				} else if (Integer.class == src.dataType) {
					b = (Integer) (src1.bottomItems[i]) <= (Integer) (src2.bottomItems[j]);
				} else if (Long.class == src.dataType || java.sql.Timestamp.class == src.dataType) {
					b = (Long) (src1.bottomItems[i]) <= (Long) (src2.bottomItems[j]);
				} else if (Float.class == src.dataType) {
					b = (Float) (src1.bottomItems[i]) <= (Float) (src2.bottomItems[j]);
				} else if (Boolean.class == src.dataType) {
					b = convertBoolean((Boolean) (src1.topItems[i])) <= convertBoolean((Boolean) (src2.topItems[j]));
				} else {
					throw new RuntimeException("Not support yet!");
				}
				if (b) {
					src.bottomItems[i + j] = src1.bottomItems[i];
					i++;
				} else {
					src.bottomItems[i + j] = src2.bottomItems[j];
					j++;
				}
			}
		}

		if (null != src1.freq && null != src2.freq) {
			src.freq = new TreeMap <Object, Long>();
			Iterator <Map.Entry <Object, Long>> it;
			it = src1.freq.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry <Object, Long> e = it.next();
				src.freq.put(e.getKey(), e.getValue());
			}
			it = src2.freq.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry <Object, Long> e = it.next();
				if (src.freq.containsKey(e.getKey())) {
					src.freq.put(e.getKey(), src.freq.get(e.getKey()) + e.getValue());
				} else {
					src.freq.put(e.getKey(), e.getValue());
				}
			}
		}

		src.itvcalc = IntervalCalculator.combine(src1.itvcalc, src2.itvcalc);

		return src;

	}

	private static int convertBoolean(Boolean val) {
		return val ? 1 : 0;
	}

	/**
	 * *
	 * 数据的和
	 */
	public double sum() {
		if (dataType == String.class || dataType == Date.class) {
			return 0;
		}
		return this.sum;
	}

	/**
	 * *
	 * 数据的平方和
	 */
	public double sum2() {
		if (dataType == String.class || dataType == Date.class) {
			return 0;
		}
		return this.sum2;
	}

	/**
	 * *
	 * 数据的立方和
	 */
	public double sum3() {
		if (dataType == String.class || dataType == Date.class) {
			return 0;
		}
		return this.sum3;
	}

	/**
	 * *
	 * 数据的四次方和
	 */
	public double sum4() {
		if (dataType == String.class || dataType == Date.class) {
			return 0;
		}
		return this.sum4;
	}

	/**
	 * *
	 * 最小值对应的双精度浮点值
	 */
	public double minDouble() {
		//        return FileJsonUtil.getDoubleValue(min, this.colType);
		if (count == 0) {
			return 0;
		}
		if (min == null) {
			System.out.println();
		}
		return StatisticUtil.getDoubleValue(min);
	}

	/**
	 * *
	 * 最大值对应的双精度浮点值
	 */
	public double maxDouble() {
		//        return FileJsonUtil.getDoubleValue(max, this.colType);
		if (count == 0) {
			return 0;
		}
		return StatisticUtil.getDoubleValue(max);
	}

	/**
	 * *
	 * 极差所对应的双精度浮点值
	 */
	public double rangeDouble() {
		if (dataType == String.class || dataType == Date.class) {
			return 0;
		}
		return maxDouble() - minDouble();
	}

	/**
	 * *
	 * 最小值
	 */
	public Object min() {
		if (dataType == String.class) {
			return 0;
		}
		return this.min;
	}

	/**
	 * *
	 * 最大值
	 */
	public Object max() {
		if (dataType == String.class) {
			return 0;
		}
		return this.max;
	}

	/**
	 * *
	 * 极差
	 *
	 * @return
	 */
	public Object range() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class) {
			return 0;
		}
		if (0 == this.count) {
			return 0;
		}
		if (Double.class == this.dataType) {
			return (Double) (max) - (Double) (min);
		} else if (Integer.class == this.dataType) {
			return (Integer) (max) - (Integer) (min);
		} else if (Long.class == this.dataType) {
			return (Long) (max) - (Long) (min);
		} else if (Float.class == this.dataType) {
			return (Float) (max) - (Float) (min);
		} else if (Boolean.class == this.dataType) {
			return maxDouble() - minDouble();
		} else {
			throw new RuntimeException("Not implemented yet!");
		}
	}

	/**
	 * *
	 * 均值
	 */
	public double mean() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class) {
			return 0;
		}
		if (count == 0) {
			return 0;
		} else {
			return sum / count;
		}
	}

	/**
	 * *
	 * 方差
	 */
	public double variance() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class) {
			return 0.0;
		}
		if (count == 0) {
			return 0.0;
		}
		if (1 == count || max.equals(min)) {
			return 0.0;
		} else {
			return Math.max(0.0, (sum2 - mean() * sum) / (count - 1));
		}
	}

	/**
	 * *
	 * 标准差
	 */
	public double standardDeviation() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class) {
			return 0.0;
		}
		return Math.sqrt(variance());
	}

	/**
	 * *
	 * 变异系数 Coefficient of Variation
	 */
	public double cv() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class || dataType == Boolean.class) {
			return 0.0;
		}
		return standardDeviation() / mean();
	}

	/**
	 * *
	 * 标准误
	 */
	public double standardError() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class || dataType == Boolean.class) {
			return 0.0;
		}
		return standardDeviation() / Math.sqrt(count);
	}

	/**
	 * *
	 * 偏度Skewness
	 */
	public double skewness() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class || dataType == Boolean.class) {
			return 0.0;
		}
		if (count == 0) {
			return Double.NaN;
		}
		if (1 == count || max.equals(min)) {
			return 0.0;
		} else {
			if (centralMoment2() <= 0) {
				return 0;
			}
			return centralMoment3() / (centralMoment2() * Math.sqrt(centralMoment2()));
		}
	}

	/**
	 * *
	 * 峰度Kurtosis
	 */
	public double kurtosis() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class || dataType == Boolean.class) {
			return 0.0;
		}
		if (count == 0) {
			return Double.NaN;
		}
		if (1 == count || max.equals(min)) {
			return 0.0;
		} else {
			if (centralMoment2() == 0) {
				return 0;
			}
			return centralMoment4() / (centralMoment2() * centralMoment2()) - 3;
		}
	}

	/**
	 * *
	 * 2阶原点矩
	 */
	public double moment2() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class || dataType == Boolean.class) {
			return 0.0;
		}
		if (count == 0) {
			return 0;
		}
		return sum2 / count;
	}

	//    /**
	//     * *
	//     * 直方图数据, 只对mtable
	//     */
	//    HistoData histoData = null;

	/**
	 * *
	 * 3阶原点矩
	 */
	public double moment3() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class || dataType == Boolean.class) {
			return 0.0;
		}
		if (count == 0) {
			return 0;
		}
		return sum3 / count;
	}
	//--------------------------------------------

	/**
	 * *
	 * 4阶原点矩
	 */
	public double moment4() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class || dataType == Boolean.class) {
			return 0.0;
		}
		if (count == 0) {
			return 0;
		}
		return sum4 / count;
	}

	/**
	 * *
	 * 2阶中心矩
	 */
	public double centralMoment2() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class || dataType == Boolean.class) {
			return 0.0;
		}
		if (1 == count || 0 == count || max.equals(min)) {
			return 0.0;
		} else {
			return (sum2 - mean() * sum) / count;
		}
	}

	/**
	 * *
	 * 3阶中心矩
	 */
	public double centralMoment3() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class || dataType == Boolean.class) {
			return 0.0;
		}
		if (1 == count || 0 == count  || max.equals(min)) {
			return 0.0;
		} else {
			if (count == 0) {
				return 0;
			}
			return (sum3 - 3 * sum2 * mean() + 2 * sum * mean() * mean()) / count;
		}
	}

	/**
	 * *
	 * 4阶中心矩
	 */
	public double centralMoment4() {
		if (dataType == String.class || dataType == java.sql.Timestamp.class || dataType == Boolean.class) {
			return 0.0;
		}
		if (1 == count || 0 == count  || max.equals(min)) {
			return 0.0;
		} else {
			if (count == 0) {
				return 0;
			}
			double mean = mean();
			return (sum4 - 4 * sum3 * mean + 6 * sum2 * mean * mean - 3 * sum * mean * mean * mean) / count;
		}
	}

	/**
	 * *
	 * 众数，在频数信息存在的前提下，可计算出此值
	 */
	//    private Object mode = null;
	public Object mode() {
		if (null != freq) {
			ArrayList <Map.Entry <Object, Long>> list = new ArrayList <Map.Entry <Object, Long>>(freq.entrySet());
			Map.Entry <Object, Long> mode = Collections.max(list, new Comparator <Map.Entry <Object, Long>>() {

				@Override
				public int compare(Entry <Object, Long> arg0, Entry <Object, Long> arg1) {
					return (int) (arg0.getValue() - arg1.getValue());
				}
			});
			return mode.getKey();
		} else {
			return null;
		}
	}

	public SRC toSRC() {
		SRC src = new SRC();
		src.dataType = this.dataType.getCanonicalName();
		src.countTotal = this.countTotal;
		src.count = this.count;
		src.countMissValue = this.countMissValue;
		src.countNanValue = this.countNanValue;
		src.countPositiveInfinity = this.countPositiveInfinity;
		src.countNegativInfinity = this.countNegativInfinity;
		src.sum = this.sum;
		src.sum2 = this.sum2;
		src.sum3 = this.sum3;
		src.sum4 = this.sum4;

		src.min = this.min;
		src.max = this.max;

		if (src.count == 0 || this.dataType == String.class ||
			this.dataType == java.sql.Timestamp.class ||
			this.dataType == Boolean.class) {
			src.mean = 0;
			src.variance = 0;
			src.standardError = 0;
			src.standardVariance = 0;
			src.skewness = 0;
		} else {
			src.mean = mean();
			src.variance = variance();
			src.standardError = standardError();
			src.standardVariance = standardDeviation();
			src.skewness = skewness();
		}

		src.normL1 = norm1;
		src.normL2 = Math.sqrt(sum2);
		return src;
	}

	public String toSrcCsv() {
		//count,sum,min,max,mean,variance,standardDeviation,standardError,skewness,missingCount

		//System.out.println("count:" + this.count);
		StringBuilder sbd = new StringBuilder();

		if (this.dataType == Double.class
			|| this.dataType == Long.class
			|| this.dataType == Integer.class
			|| this.dataType == long.class
			|| this.dataType == int.class
			|| this.dataType == double.class) {

			sbd.append(this.count).append(",")
				.append(this.sum).append(",")
				.append(this.minDouble()).append(",")
				.append(this.maxDouble()).append(",")
				.append(this.mean()).append(",")
				.append(this.variance()).append(",")
				.append(this.standardDeviation()).append(",")
				.append(this.standardError()).append(",")
				.append(this.skewness()).append(",")
				.append(this.countMissValue);
		} else {
			sbd.append(this.count).append(",")
				.append(this.countMissValue);

		}
		return sbd.toString();
	}

	public String toTopKJson() {
		StringBuilder sbd = new StringBuilder();
		int topk = 10;
		for (int i = 0; i < topk; i++) {
			if (i < topItems.length) {
				sbd.append(Double.parseDouble(String.valueOf(topItems[i])));
			}
			if (i != topk - 1) {
				sbd.append(",");
			}
		}
		return sbd.toString();
		//        JsonObject z = new JsonObject();
		//        for (int i = 0; i < topItems.length; i++) {
		//            z.addProperty("top" + i, Double.parseDouble(String.valueOf(topItems[i])));
		//        }
		//        return gson.toJson(z);
	}

	public String toBottomKJson() {
		StringBuilder sbd = new StringBuilder();
		int topk = 10;
		for (int i = 0; i < topk; i++) {
			if (i < bottomItems.length) {
				sbd.append(Double.parseDouble(String.valueOf(bottomItems[i])));
			}
			if (i != topk - 1) {
				sbd.append(",");
			}
		}
		return sbd.toString();
		//        JsonObject z = new JsonObject();
		//        for (int i = 0; i < topItems.length; i++) {
		//            z.addProperty("bottom" + i, Double.parseDouble(String.valueOf(topItems[i])));
		//        }
		//        return gson.toJson(z);
	}

	public String toFreqJson(long timestamp) {
		TreeMap <Object, Long> freq = new TreeMap <>();
		try {
			freq = getFrequencyMap();
		} catch (Exception e) {
			e.printStackTrace();
		}

		double countTotal = 0;

		StackedBar stackedBar = new StackedBar();
		if (freq != null) {
			Set <String> xTags = new HashSet <String>();

			Object[] objs = freq.keySet().toArray();

			for (int k = 0; k < objs.length; k++) {
				String xTag = String.valueOf(objs[k]);
				xTags.add(xTag);
				countTotal += (double) freq.get(objs[k]);
			}

			String[] xTagsVec = xTags.toArray(new String[0]);
			for (int i = 0; i < xTagsVec.length; i++) {
				Map <String, Object> colMap = new HashMap <String, Object>();
				colMap.put(colName, xTagsVec[i]);
				stackedBar.data.add(colMap);
			}

			stackedBar.xTag = new String[1];
			stackedBar.xTag[0] = String.valueOf(timestamp);
			double count = countTotal;

			for (int k = 0; k < objs.length; k++) {
				String xTag = String.valueOf(objs[k]);
				double value = Double.parseDouble(String.valueOf(freq.get(objs[k])));
				int idx = TableUtil.findColIndex(xTagsVec, xTag);
				if (count != 0) {
					stackedBar.data.get(idx).put(String.valueOf(timestamp), value / count);
				} else {
					stackedBar.data.get(idx).put(String.valueOf(timestamp), value);
				}
			}
		}

		return gson.toJson(stackedBar);
	}

	public String toHistogramJson(long timestamp) {
		List <Histogram> histos = new ArrayList <Histogram>();

		Histogram histo = new Histogram();
		histo.name = String.valueOf(timestamp);
		IntervalCalculator ic = null;
		try {
			ic = this.getIntervalCalculator();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		double step = ic.getStep().doubleValue();
		long[] counts = ic.getCount();

		histo.step = step;
		histo.intervals = new com.alibaba.alink.common.viz.plot.Interval[counts.length];
		for (int i = 0; i < counts.length; i++) {
			histo.intervals[i] = new com.alibaba.alink.common.viz.plot.Interval();
			histo.intervals[i].depth = new double[2];
			histo.intervals[i].depth[0] = ic.getTag(i).doubleValue();
			histo.intervals[i].depth[1] = ic.getTag(i + 1).doubleValue();
			histo.intervals[i].count = counts[i];
		}

		histos.add(histo);
		return gson.toJson(histos);
	}

	void init(Class dataType, long countTotal, long count, long countMissValue, long countNanValue,
			  long countPositiveInfinity, long countNegativInfinity,
			  double sum, double absSum, double sum2, double sum3, double sum4, Object minValue, Object maxValue) {
		this.dataType = dataType;
		if (countTotal != count + countMissValue + countNanValue + countPositiveInfinity + countNegativInfinity) {
			throw new RuntimeException();
		}
		this.countTotal = countTotal;
		this.count = count;
		this.countMissValue = countMissValue;
		this.countNanValue = countNanValue;
		this.countPositiveInfinity = countPositiveInfinity;
		this.countNegativInfinity = countNegativInfinity;
		this.sum = sum;
		this.sum2 = sum2;
		this.sum3 = sum3;
		this.sum4 = sum4;
		this.norm1 = absSum;

		this.min = minValue;
		this.max = maxValue;
	}

	public void combine(SummaryResultCol src) throws CloneNotSupportedException {
		if (!dataType.equals(src.dataType)) {
			throw new RuntimeException("The 2 col data types are not equal!");
		}

		if (0 == countTotal) {

		} else if (0 == src.countTotal) {
			return;
		}

		Object minValue = null;
		Object maxValue = null;
		if (dataType == Double.class) {
			minValue = (Double) (min) < (Double) (src.min) ? min : src.min;
			maxValue = (Double) (max) > (Double) (src.max) ? max : src.max;
		} else if (dataType == Float.class) {
			minValue = (Float) (min) < (Float) (src.min) ? min : src.min;
			maxValue = (Float) (max) > (Float) (src.max) ? max : src.max;
		} else if (dataType == Long.class) {
			minValue = (Long) (min) < (Long) (src.min) ? min : src.min;
			maxValue = (Long) (max) > (Long) (src.max) ? max : src.max;
		} else if (dataType == Short.class) {
			minValue = (Short) (min) < (Short) (src.min) ? min : src.min;
			maxValue = (Short) (max) > (Short) (src.max) ? max : src.max;
		} else if (dataType == Byte.class) {
			minValue = (Byte) (min) < (Byte) (src.min) ? min : src.min;
			maxValue = (Byte) (max) > (Byte) (src.max) ? max : src.max;
		} else if (dataType == Integer.class) {
			minValue = (Integer) (min) < (Integer) (src.min) ? min : src.min;
			maxValue = (Integer) (max) > (Integer) (src.max) ? max : src.max;
		} else if (dataType == String.class) {

		} else {
			throw new RuntimeException("Not implemented yet!");
		}

		this.countTotal = countTotal + src.countTotal;
		this.count = count + src.count;
		this.countMissValue = countMissValue + src.countMissValue;
		this.countNanValue = countNanValue + src.countNanValue;
		this.countPositiveInfinity = countPositiveInfinity + src.countPositiveInfinity;
		this.countNegativInfinity = countNegativInfinity + src.countNegativInfinity;
		this.sum = sum + src.sum;
		this.sum2 = sum2 + src.sum2;
		this.sum3 = sum3 + src.sum3;
		this.sum4 = sum4 + src.sum4;

		this.min = minValue;
		this.max = maxValue;
	}

	public boolean hasFreq() {
		if (this.freq != null) {
			return true;
		}
		if (count == 0) {
			return true;
		}
		return false;
	}

	/**
	 * *
	 * 获取频数信息
	 *
	 * @return 频数信息
	 * @throws Exception
	 */
	public TreeMap <Object, Long> getFrequencyMap() {
		return this.freq;
	}

	/**
	 * *
	 * 获取按数值排序的频数信息
	 *
	 * @return 按数值排序的频数信息
	 * @throws Exception
	 */
	public ArrayList <Entry <Object, Long>> getFrequencyOrderByItem() {
		if (null == freq) {
			return null;
		}
		return new ArrayList <Entry <Object, Long>>(freq.entrySet());
	}

	/**
	 * *
	 * 获取按计数个数排序的频数信息
	 *
	 * @return 按计数个数排序的频数信息
	 * @throws Exception
	 */
	public ArrayList <Entry <Object, Long>> getFrequencyOrderByCount() {
		if (null == freq) {
			return null;
		}
		ArrayList <Map.Entry <Object, Long>> list = new ArrayList <Map.Entry <Object, Long>>(freq.entrySet());
		Collections.sort(list, new Comparator <Map.Entry <Object, Long>>() {

			public int compare(Entry <Object, Long> arg0, Entry <Object, Long> arg1) {
				return (int) (arg0.getValue() - arg1.getValue());
			}
		});
		return list;
	}

	/**
	 * *
	 * 获取基本直方图区间划分及区间内元素个数
	 *
	 * @return 基本直方图区间划分及区间内元素个数
	 * @throws Exception
	 */
	public IntervalCalculator getIntervalCalculator() {
		return this.itvcalc;
	}

	/**
	 * @return 百分位数信息
	 */
	public Percentile getPercentile() {
		if (!hasFreq()) {
			return null;
		}
		if (this.count == 0) {
			return null;
		}
		Quantile qtl = Quantile.fromFreqSet(100, this.dataType, freq);
		Percentile pct = new Percentile(this.dataType);
		pct.items = qtl.items;
		pct.median = pct.items[50];
		pct.Q1 = pct.items[25];
		pct.Q3 = pct.items[75];
		pct.min = pct.items[0];
		pct.max = pct.items[100];

		return pct;
	}

	/**
	 * * 获取近似百分位数
	 *
	 * @return 近似百分位数
	 */
	public ApproximatePercentile getApproximatePercentile() {
		if (this.count == 0) {
			return null;
		}
		if (hasFreq()) {
			return new ApproximatePercentile(getPercentile());
		} else {
			return new ApproximatePercentile(getIntervalCalculator(), minDouble(), maxDouble());
		}
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();
		sbd.append("DataType           : ");
		sbd.append(dataType.getSimpleName());
		sbd.append("\nCount              : ");
		sbd.append(count);
		sbd.append("\nMean               : ");
		sbd.append(mean());
		sbd.append("\nVariance           : ");
		sbd.append(variance());
		sbd.append("\nStandardScaler Deviation : ");
		sbd.append(standardDeviation());
		sbd.append("\nCoefficient of Variation : ");
		sbd.append(cv());
		sbd.append("\nStandardScaler Error     : ");
		sbd.append(standardError());
		sbd.append("\nSum                : ");
		sbd.append(sum);

		sbd.append("\nMin                : ");
		sbd.append(min());
		sbd.append("\nMax                : ");
		sbd.append(max());
		sbd.append("\nRange              : ");
		sbd.append(range());

		sbd.append("\n2nd Moment         : ");
		sbd.append(moment2());
		sbd.append("\n3rd Moment         : ");
		sbd.append(moment3());
		sbd.append("\n4th Moment         : ");
		sbd.append(moment4());
		sbd.append("\n2nd Central Moment : ");
		sbd.append(centralMoment2());
		sbd.append("\n3rd Central Moment : ");
		sbd.append(centralMoment3());
		sbd.append("\n4th Central Moment : ");
		sbd.append(centralMoment4());
		sbd.append("\nSkewness           : ");
		sbd.append(skewness());
		sbd.append("\nKurtosis           : ");
		sbd.append(kurtosis());

		if (topItems != null && topItems.length > 0) {
			sbd.append("\n[Top ").append(this.topItems.length).append(" value]");
			for (int i = 0; i < this.topItems.length; i++) {
				sbd.append("\n").append(topItems[i]);
			}
		}
		if (bottomItems != null && bottomItems.length > 0) {
			sbd.append("\n[Bottom ").append(this.bottomItems.length).append(" value]");
			for (int i = 0; i < this.bottomItems.length; i++) {
				sbd.append("\n").append(bottomItems[i]);
			}
		}

		return sbd.toString();
	}

	public SummaryResultCol copy() {
		SummaryResultCol srcCopy = new SummaryResultCol();
		srcCopy.dataType = dataType;
		srcCopy.countTotal = countTotal;
		srcCopy.count = count;
		srcCopy.countMissValue = countMissValue;
		srcCopy.countNanValue = countNanValue;
		srcCopy.countNegativInfinity = countNegativInfinity;
		srcCopy.countPositiveInfinity = countPositiveInfinity;
		srcCopy.sum = sum;
		srcCopy.sum2 = sum2;
		srcCopy.sum3 = sum3;
		srcCopy.sum4 = sum4;
		srcCopy.min = min;
		srcCopy.max = max;
		srcCopy.topItems = topItems;
		srcCopy.bottomItems = bottomItems;
		srcCopy.freq = freq;
		srcCopy.itvcalc = itvcalc;
		srcCopy.colName = colName;
		return srcCopy;
	}
}
