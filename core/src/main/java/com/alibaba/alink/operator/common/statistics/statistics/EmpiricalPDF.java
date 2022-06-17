package com.alibaba.alink.operator.common.statistics.statistics;

import com.alibaba.alink.common.probabilistic.PDF;

import java.util.ArrayList;
import java.util.TreeMap;

public class EmpiricalPDF {

	public int n;
	public double[] f;
	public double[] x;
	public int sampleNum = 100;
	public double[] sample = new double[sampleNum];
	public double[] sampleX = new double[sampleNum];
	public String colName = null;

	public EmpiricalPDF(SummaryResultCol src) throws Exception {
		if (src.hasFreq()) {
			initFreq(src.getFrequencyMap(), src.dataType);
		} else {
			initIC(src.getIntervalCalculator());
		}
		colName = src.colName;
	}

	void initIC(IntervalCalculator ic) throws Exception {
		double start = ic.getLeftBound().doubleValue();
		double step = ic.getStep().doubleValue();
		long[] counts = ic.getCount();
		double s = 0.0;
		for (long count : counts) {
			s += count;
		}

		int n2 = counts.length;

		int nCountMin = 10;
		int nCountMax = 100;
		int nCount = 0;
		if (s > 10000) {
			nCount = nCountMax;
		} else {
			nCount = nCountMin;
		}
		int step2 = Math.max(1, n2 / nCount);

		ArrayList f2 = new ArrayList();
		ArrayList x2 = new ArrayList();

		int k = 0;
		double s2 = 0;
		double t2 = 0;

		for (int i = 0; i < n2; i++) {
			if (k < step2) {
				s2 += (double) counts[i];
				t2 += start + step * i;
				k++;
				if (i == (n2 - 1) && n2 % step2 != 0) {
					f2.add(s2);
					x2.add(t2 / (n2 % step2));
				}
			} else {
				f2.add(s2);
				x2.add(t2 / step2);
				i--;
				s2 = 0;
				t2 = 0;
				k = 0;
			}
		}

		ArrayList f3 = new ArrayList();
		ArrayList x3 = new ArrayList();

		for (int i = 0; i < f2.size(); i++) {
			f3.add(f2.get(i));
			x3.add(x2.get(i));
		}

		this.n = f3.size();
		this.f = new double[n];
		this.x = new double[n];

		for (int i = 0; i < f3.size(); i++) {
			f[i] = (Double) f3.get(i) / s;
			x[i] = (Double) x3.get(i);
		}

		double StandardDevition = calculateStandardDevitionofHistogram(start, step, counts);
		double sigma = 1.06 * StandardDevition * Math.pow(s, -0.2);
		PDF func = new PDF();
		double begin = start;
		double end = start + step * counts.length;

		double sampleXstart = begin;
		double sampleStep = (end - begin) / (sampleNum - 1);
		for (int j = 0; j < sampleNum; j++) {
			sample[j] = 0.0;
			for (int i = 0; i < this.n; i++) {
				sample[j] += f[i] * func.normal(sampleXstart, x[i], sigma * sigma);
			}
			sampleX[j] = sampleXstart;
			sampleXstart += sampleStep;
		}
	}

	void initFreq(TreeMap <Object, Long> freqTree, Class dataType) throws Exception {
		TreeMap <Object, Long> MedianTree = new TreeMap();
		Quantile quan = new Quantile(100, Object.class);
		Quantile qtl = quan.fromFreqSet(100, Object.class, freqTree);
		double median = 0.0;
		median = Double.parseDouble(String.valueOf(qtl.getQuantile(50)));

		Object[] keySet = freqTree.keySet().toArray();
		for (int i = 0; i < freqTree.size(); i++) {
			long count = 0;
			double a = Double.parseDouble(String.valueOf(keySet[i]));
			double b = Math.abs(a - median);
			if (MedianTree.containsKey(b)) {
				count = MedianTree.get(b) + freqTree.get(keySet[i]);
			} else {
				count = freqTree.get(keySet[i]);
			}
			MedianTree.put(b, count);
		}

		Quantile qtm = quan.fromFreqSet(100, Double.class, MedianTree);
		median = Double.parseDouble(String.valueOf(qtm.getQuantile(50)));

		this.n = freqTree.size();
		this.f = new double[n];
		this.x = new double[n];
		long count = 0;
		double max = Double.MIN_VALUE;
		double min = Double.MAX_VALUE;
		double s = 0;

		if (freqTree.size() != 1) {
			for (int i = 0; i < freqTree.size(); i++) {
				x[i] = Double.parseDouble(String.valueOf(keySet[i]));
				f[i] = freqTree.get(keySet[i]);
				count += f[i];
			}
			double sigma;
			if (median <= 0.0) {
				median = Double.parseDouble(String.valueOf(qtl.getQuantile(100)))
					- Double.parseDouble(String.valueOf(qtl.getQuantile(0)));
			}

			if (median > 0) {
				sigma = 1.06 * median * Math.pow(count, -0.2) / 0.6745;
			} else {
				sigma = 1.0;
			}

			for (int i = 0; i < x.length; i++) {
				if (max < x[i]) {
					max = x[i];
				}
				if (min > x[i]) {
					min = x[i];
				}
			}
			double step = (max - min) / (sampleNum - 1);
			double begin = min;

			PDF func = new PDF();
			for (int i = 0; i < sampleNum; i++) {
				sampleX[i] = begin + step * i;
				for (int j = 0; j < freqTree.size(); j++) {
					sample[i] += f[j] * func.normal(sampleX[i], x[j], sigma * sigma) / count;
				}
			}
		} else {
			count += freqTree.get(keySet[0]);
			double center = Double.parseDouble(String.valueOf(keySet[0]));
			double sigma;
			if (median <= 0.0) {
				median = Double.parseDouble(String.valueOf(qtl.getQuantile(100)))
					- Double.parseDouble(String.valueOf(qtl.getQuantile(0)));

			}

			if (median > 0) {
				sigma = 1.06 * median * Math.pow(count, -0.2) / 0.6745;
			} else {
				sigma = 1.0;
			}
			max = center + 3 * sigma;
			min = center - 3 * sigma;
			double step, begin;
			step = (max - min) / (sampleNum - 1);
			begin = min;

			PDF func = new PDF();
			for (int i = 0; i < sampleNum; i++) {
				sampleX[i] = begin + step * i;
				sample[i] += func.normal(sampleX[i], center, sigma * sigma);
			}
		}
	}

	public double calculateStandardDevitionofHistogram(double start, double step, long[] hist) {

		double sum = 0.0, StandardDevition = 0.0, average = 0.0, total = 0.0, interval = 0.0;

		for (int i = 0; i < hist.length; i++) {
			interval = ((start + i * step) + (start + (i + 1) * step)) * 0.5;
			sum += hist[i] * interval;
			total += hist[i];
		}

		average = sum / total;
		sum = 0.0;

		for (int i = 0; i < hist.length; i++) {
			interval = ((start + i * step) + (start + (i + 1) * step)) * 0.5;
			sum += hist[i] * (interval - average) * (interval - average);
		}

		StandardDevition = Math.sqrt(sum / (total - 1));

		return StandardDevition;
	}
}
