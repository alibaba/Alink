package com.alibaba.alink.operator.common.statistics.statistics;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.utils.AlinkSerializable;

/**
 * @author yangxu
 */
public class Percentile implements AlinkSerializable {

	public Class dataType;
	public Object[] items = new Object[101];
	public Object median;
	public Object Q1;
	public Object Q3;
	public Object min;
	public Object max;

	Percentile(Class dataType) {
		this.dataType = dataType;
	}

	public Object getPercentile(int k) {
		if (k < 0 || k > 100) {
			throw new AkIllegalOperatorParameterException(String.format(
				"k must in [0, 100], but k is [%s].", k));
		}
		return items[k];
	}

	@Override
	public String toString() {
		java.io.CharArrayWriter cw = new java.io.CharArrayWriter();
		java.io.PrintWriter pw = new java.io.PrintWriter(cw, true);
		pw.print("Min                 : ");
		pw.println(min);
		pw.print("Lower Quartile (Q1) : ");
		pw.println(Q1);
		pw.print("Median              : ");
		pw.println(median);
		pw.print("Upper Quartile (Q3) : ");
		pw.println(Q3);
		pw.print("Max                 : ");
		pw.println(max);
		for (int i = 0; i <= 100; i++) {
			pw.print(i);
			pw.print("% : ");
			pw.println(items[i]);
		}

		return cw.toString();
	}

}
