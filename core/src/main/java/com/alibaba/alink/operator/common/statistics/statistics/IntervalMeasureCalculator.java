package com.alibaba.alink.operator.common.statistics.statistics;

import java.io.Serializable;

/**
 * @author yangxu
 */
public class IntervalMeasureCalculator implements Serializable {

	private static final long serialVersionUID = -4625259870390900630L;
	/**
	 * *
	 * 数据个数
	 */
	public long count;
	/**
	 * *
	 * 数据和
	 */
	public double sum;
	/**
	 * *
	 * 数据平方和
	 */
	public double sum2;
	/**
	 * *
	 * 数据立方和
	 */
	public double sum3;
	/**
	 * *
	 * 数据四次方和
	 */
	public double sum4;
	/**
	 * *
	 * 最小值
	 */
	public double minDouble;
	/**
	 * *
	 * 最大值
	 */
	public double maxDouble;

	public IntervalMeasureCalculator() {
		count = 0;
		sum = 0.0;
		sum2 = 0.0;
		sum3 = 0.0;
		sum4 = 0.0;
		count = 0;
		minDouble = Double.POSITIVE_INFINITY;
		maxDouble = Double.NEGATIVE_INFINITY;
	}

	/**
	 * *
	 * 对新增的数据，增量计算各指标
	 *
	 * @param d 新增的数据
	 */
	public void calculate(double d) {
		if (d < minDouble) {
			minDouble = d;
		}
		if (d > maxDouble) {
			maxDouble = d;
		}

		sum += d;
		sum2 += d * d;
		sum3 += d * d * d;
		sum4 += d * d * d * d;
		count++;
	}

	/**
	 * *
	 * 将另一个MeasureCalculator实例的结果，增量计算到本实例。 用于多个MeasureCalculator实例的计算结果汇总。
	 *
	 * @param mc 一个MeasureCalculator实例
	 */
	public void calculate(IntervalMeasureCalculator mc) {
		if (mc.minDouble < this.minDouble) {
			this.minDouble = mc.minDouble;
		}
		if (mc.maxDouble > this.maxDouble) {
			this.maxDouble = mc.maxDouble;
		}

		this.sum += mc.sum;
		this.sum2 += mc.sum2;
		this.sum3 += mc.sum3;
		this.sum4 += mc.sum4;
		this.count += mc.count;
	}

	public SummaryResultCol getFinalResult() {
		SummaryResultCol src = new SummaryResultCol();
		src.init(Double.class, count, count, 0, 0,
			0, 0, 0, sum, 0.0, sum2,
			sum3, sum4, Double.valueOf(minDouble), Double.valueOf(maxDouble));
		return src;
	}

	@Override
	public String toString() {
		String result = "";
		result += String.valueOf(count);
		result += " ";
		result += String.valueOf(sum);
		result += " ";
		result += String.valueOf(sum2);
		result += " ";
		result += String.valueOf(sum3);
		result += " ";
		result += String.valueOf(sum4);
		result += " ";
		result += String.valueOf(minDouble);
		result += " ";
		result += String.valueOf(maxDouble);
		return result;
	}
}
