package com.alibaba.alink.operator.common.statistics.statistics;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * q-Quantiles
 *
 * @author yangxu
 */
public class Quantile implements AlinkSerializable {

	public final Class dataType;
	public final int q;
	public Object[] items = null;

	/**
	 * *
	 * 构造函数
	 *
	 * @param q        分位数个数，即指定q-分位数
	 * @param dataType 数据类型
	 */
	Quantile(int q, Class dataType) {
		this.q = q;
		this.dataType = dataType;
		this.items = new Object[q + 1];
	}

	public static Quantile fromFreqSet(int q, Class dataType, TreeMap <Object, Long> freq) {
		Quantile qtl = new Quantile(q, dataType);
		long n = 0;
		long[] cntScan = new long[freq.size()];
		Object[] item = new Object[freq.size()];
		int k = 0;
		Iterator <Entry <Object, Long>> it = freq.entrySet().iterator();
		while (it.hasNext()) {
			Entry <Object, Long> e = it.next();
			item[k] = e.getKey();
			n += e.getValue().longValue();
			cntScan[k] = n - 1;
			k++;
		}
		if (n <= 0) {
			throw new RuntimeException();
		}
		double t = 0.0;
		for (int i = 0; i <= q; i++) {
			t = 1.0 * i / q;
			if (t < 0.0) {
				qtl.items[i] = item[0];
			} else if (t >= 1.0) {
				qtl.items[i] = item[item.length - 1];
			} else {
				qtl.items[i] = item[getItemIndex(cntScan, (long) ((n - 1) * t))];
			}
		}

		return qtl;
	}

	private static int getItemIndex(long[] cntScan, long k) {
		int low = 0;
		int high = cntScan.length - 1;
		int cur;
		if (k > cntScan[high]) {
			throw new RuntimeException();
		}
		if (k <= cntScan[0]) {
			return 0;
		}
		while (low < high - 1) {
			cur = (low + high) / 2;
			if (k <= cntScan[cur]) {
				high = cur;
			} else {
				low = cur;
			}
		}
		return high;
	}

	public Object getQuantile(int k) {
		if (k < 0 || k > q) {
			throw new RuntimeException();
		}
		return items[k];
	}

	@Override
	public String toString() {
		java.io.CharArrayWriter cw = new java.io.CharArrayWriter();
		java.io.PrintWriter pw = new java.io.PrintWriter(cw, true);
		for (int i = 0; i <= q; i++) {
			pw.println(i + "-th element of " + q + "-QuantileWindowFunction : " + items[i]);
		}
		return cw.toString();
	}

}
