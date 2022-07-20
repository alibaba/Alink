/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.operator.common.statistics.statistics;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.statistics.StatisticUtil;
import com.alibaba.alink.operator.common.statistics.basicstat.WindowTable;
import com.alibaba.alink.params.statistics.HasStatLevel_L1;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 * @author yangxu
 */
public class Summary2 {

	//for stream: stream iter can iter two times.
	public static SummaryResultTable streamSummary(WindowTable wt, String[] statColNames,
												   int smallK, int largeK, int freqSize, int histogramBins,
												   HasStatLevel_L1.StatLevel statLevel) {
		return basicSummary(wt, statColNames, smallK, largeK, freqSize, histogramBins, statLevel);
	}

	//for batch: batch iter can iter only one times.
	public static SummaryResultTable batchSummary(WindowTable wt, String[] statColNames,
												  int smallK, int largeK, int freqSize, int histogramBins,
												  HasStatLevel_L1.StatLevel statLevel) {
		return batchSummary(wt, statColNames,
			smallK, largeK, freqSize, histogramBins,
			statLevel,
			10000);
	}

	static SummaryResultTable batchSummary(WindowTable wt, String[] statColNames,
										   int smallK, int largeK, int freqSize, int histogramBins,
										   HasStatLevel_L1.StatLevel statLevel,
										   int batchSize) {
		List <Row> data = new ArrayList <>();
		Iterator <Row> iter = wt.getIterator();
		int count = 0;
		SummaryResultTable srt = null;
		if (statColNames == null) {
			statColNames = wt.colNames;
		}
		while (iter.hasNext()) {
			data.add(iter.next());
			count++;
			if (count == batchSize) {
				SummaryResultTable srtUnit = basicSummary(new WindowTable(wt.colNames, wt.colTypes, data),
					statColNames, smallK, largeK, freqSize, histogramBins, statLevel);
				if (srt == null) {
					srt = srtUnit;
				} else {
					srt = SummaryResultTable.combine(srt, srtUnit);
				}
				data.clear();
				count = 0;
			}
		}
		if (count != 0) {
			SummaryResultTable srtUnit = basicSummary(new WindowTable(wt.colNames, wt.colTypes, data),
				statColNames, smallK, largeK, freqSize, histogramBins, statLevel);
			if (srt == null) {
				srt = srtUnit;
			} else {
				srt = SummaryResultTable.combine(srt, srtUnit);
			}
		}

		return srt;
	}

	//one Summary
	static SummaryResultTable basicSummary(WindowTable wt, String[] statColNames,
										   int smallK, int largeK,
										   int freqSize, int histogramBins,
										   HasStatLevel_L1.StatLevel statLevel) {

		if (wt.colNames == null || wt.colNames.length == 0) {
			throw new AkIllegalOperatorParameterException("colNames must not be empty.");
		}
		if (wt.colTypes == null || wt.colTypes.length == 0) {
			throw new AkIllegalOperatorParameterException("colTypes must not be empty.");
		}
		if (wt.colNames.length != wt.colTypes.length) {
			throw new AkIllegalOperatorParameterException("colNames length must equal with colTypes length.");
		}

		if (statColNames == null || statColNames.length == 0) {
			statColNames = wt.colNames;
		}

		boolean bCov = isCalCov(statLevel);
		boolean bCalcFreq = isCalFreq(statLevel);

		SummaryResultTable srt = new SummaryResultTable(statColNames);

		int[] idxStat = new int[statColNames.length];
		for (int i = 0; i < statColNames.length; i++) {
			idxStat[i] = TableUtil.findColIndexWithAssert(wt.colNames, statColNames[i]);
		}

		int nStat = idxStat.length;

		MeasureIteratorBase[] mis = newMeasures(wt.colTypes, idxStat);
		FrequencyIterator[] fis = newFreqs(wt.colTypes, idxStat, bCalcFreq, freqSize);
		TopKInterator[] tis = newTopK(wt.colTypes, idxStat, bCalcFreq, smallK, largeK);
		srt.dotProduction = newDotProduction(nStat, bCov);

		Row val;
		Iterator <Row> iter = wt.getIterator();
		while (iter.hasNext()) {
			val = iter.next();
			for (int i = 0; i < nStat; i++) {
				Object obj = val.getField(idxStat[i]);
				mis[i].visit(obj);
				if (tis != null && tis[i] != null) {
					tis[i].visit(obj);
				}
				if (fis != null && fis[i] != null) {
					fis[i].visit(obj);
				}
			}
		}

		for (int i = 0; i < nStat; i++) {
			mis[i].finalResult(srt.src[i]);
			if (tis != null && tis[i] != null) {
				tis[i].finalResult(srt.src[i]);
			}
			if (fis != null && fis[i] != null) {
				fis[i].finalResult(srt.src[i]);
			}
		}

		Interval[] intervals = newIntervals(srt, bCalcFreq, histogramBins);
		long[][] intervalCounts = newIntervalCounts(intervals);

		if (bCalcFreq || bCov) {
			iter = wt.getIterator();
			double[] ds = new double[nStat];
			while (iter.hasNext()) {
				updateCovAndHistogramCount(iter.next(), wt.colTypes, idxStat, intervals, ds, intervalCounts,
					srt.dotProduction);
			}
			updateInterVals(intervals, intervalCounts, srt);
		}
		return srt;
	}

	//for window
	public static SrtForWp summaryForWp(String[] colNames, Class[] colTypes, Iterable <Row> data,
										String[] statColNames, boolean[] needFreqs,
										int timeColIdx, long startTime, long endTime) {

		if (colNames == null || colNames.length == 0) {
			throw new AkIllegalOperatorParameterException("colNames must not be empty.");
		}
		if (colTypes == null || colTypes.length == 0) {
			throw new AkIllegalOperatorParameterException("colTypes must not be empty.");
		}
		if (colNames.length != colTypes.length) {
			throw new AkIllegalOperatorParameterException("colNames length must equal with colTypes length.");
		}

		SrtForWp srt = new SrtForWp(statColNames);

		int[] idxStat = TableUtil.findColIndicesWithAssertAndHint(colNames, statColNames);
		int nStat = idxStat.length;

		MeasureIteratorBase[] mis = newMeasures(colTypes, idxStat);

		DistinctValueIterator[] fis = new DistinctValueIterator[nStat];
		for (int j = 0; j < nStat; j++) {
			if (needFreqs[j]) {
				fis[j] = new DistinctValueIterator(colTypes[idxStat[j]]);
			}
		}

		for (Row vals : data) {
			if (isCalc(vals, timeColIdx, startTime, endTime)) {
				for (int i = 0; i < nStat; i++) {
					Object obj = vals.getField(idxStat[i]);
					mis[i].visit(obj);
					if (needFreqs[i]) {
						fis[i].visit(obj);
					}
				}
			}
		}

		for (int i = 0; i < nStat; i++) {
			mis[i].finalResult(srt.src[i]);
			if (needFreqs[i]) {
				srt.distinctValues[i] = fis[i].mapFreq;
			}
		}

		return srt;
	}

	private static boolean isCalFreq(HasStatLevel_L1.StatLevel statLevel) {
		return HasStatLevel_L1.StatLevel.L3 == statLevel;
	}

	private static boolean isCalCov(HasStatLevel_L1.StatLevel statLevel) {
		return HasStatLevel_L1.StatLevel.L2 == statLevel ||
			HasStatLevel_L1.StatLevel.L3 == statLevel;
	}

	private static boolean isNumber(Class colType) {
		return !(colType == String.class
			|| colType == Date.class
			|| colType == Boolean.class);
	}

	private static boolean isCalc(Row data, int timeColIdx, long startTime, long endTime) {
		if (timeColIdx != -1) {
			long timestamp = (long) data.getField(timeColIdx);
			return timestamp >= startTime && timestamp < endTime;
		} else {
			return true;
		}
	}

	private static MeasureIteratorBase[] newMeasures(Class[] colTypes, int[] idxStat) {
		int nStat = idxStat.length;
		MeasureIteratorBase[] mis = new MeasureIteratorBase[nStat];
		for (int j = 0; j < nStat; j++) {
			int index = idxStat[j];
			if (Double.class == colTypes[index] || Float.class == colTypes[index]) {
				mis[j] = new MeasureIteratorDouble(colTypes[index]);
			} else if (Long.class == colTypes[index] || Integer.class == colTypes[index]
				|| Short.class == colTypes[index] || Byte.class == colTypes[index]) {
				mis[j] = new MeasureIteratorLong(colTypes[index]);
			} else if (String.class == colTypes[index]) {
				mis[j] = new MeasureIteratorString(colTypes[index]);
			} else if (Boolean.class == colTypes[index]) {
				mis[j] = new MeasureIteratorBoolean(colTypes[index]);
			} else if (java.sql.Timestamp.class == colTypes[index]) {
				mis[j] = new MeasureIteratorDate(colTypes[index]);
			} else {
				throw new AkUnsupportedOperationException(String.format(
					"col type [%s] not supported.", colTypes[index].getSimpleName()));
			}
		}
		return mis;
	}

	private static FrequencyIterator[] newFreqs(Class[] colTypes, int[] idxStat, boolean bCalcFreq, int freqSize) {
		FrequencyIterator[] fis = null;
		if (bCalcFreq) {
			int nStat = idxStat.length;
			fis = new FrequencyIterator[nStat];
			for (int j = 0; j < nStat; j++) {
				int index = idxStat[j];
				fis[j] = new FrequencyIterator(colTypes[index], freqSize);
			}

		}
		return fis;
	}

	private static TopKInterator[] newTopK(Class[] colTypes, int[] idxStat, boolean bCalcFreq, int smallK, int
		largeK) {
		TopKInterator[] fis = null;
		if (bCalcFreq) {
			int nStat = idxStat.length;
			fis = new TopKInterator[nStat];
			for (int j = 0; j < nStat; j++) {
				int index = idxStat[j];
				if (colTypes[index] != String.class) {
					fis[j] = new TopKInterator(colTypes[index], smallK, largeK);
				}
			}
		}
		return fis;
	}

	private static double[][] newDotProduction(int nStat, boolean bCov) {
		double[][] dotProduction = null;
		if (bCov) {
			dotProduction = new double[nStat][nStat];
			for (int j = 0; j < nStat; j++) {
				for (int k = 0; k < nStat; k++) {
					dotProduction[j][k] = 0.0;
				}
			}
		}
		return dotProduction;
	}

	private static Interval[] newIntervals(SummaryResultTable srt, boolean bFreq, int histogramBins) {
		Interval[] intervals = null;
		int nStat = srt.src.length;

		if (bFreq) {
			intervals = new Interval[nStat];
			for (int j = 0; j < nStat; j++) {
				Interval r;
				if (srt.src[j].count != 0 && isNumber(srt.src[j].dataType)) {
					try {
						r = Interval.findInterval(srt.src[j].minDouble(), srt.src[j].maxDouble(), histogramBins);
					} catch (Exception ex) {
						throw new AkIllegalStateException(ex.getMessage());
					}
				} else {
					r = null;
				}
				intervals[j] = r;
			}
		}
		return intervals;
	}

	private static long[][] newIntervalCounts(Interval[] intervals) {
		if (intervals == null) {
			return null;
		}
		int nStat = intervals.length;
		long[][] values = new long[nStat][];
		for (int j = 0; j < nStat; j++) {
			if (intervals[j] != null) {
				values[j] = new long[intervals[j].NumIntervals()];
				for (int k = 0; k < values[j].length; k++) {
					values[j][k] = 0;
				}
			}
		}
		return values;
	}

	//ds, intervalCounts, dotProduction will be update.
	private static void updateCovAndHistogramCount(Row val, Class[] colTypes, int[] idxStat, Interval[] intervals,
												   double[] ds, long[][] intervalCounts, double[][] dotProduction) {
		int nStat = idxStat.length;
		for (int j = 0; j < nStat; j++) {
			if (val.getField(idxStat[j]) == null) {
				ds[j] = 0;
			} else {
				ds[j] = StatisticUtil.getDoubleValue(val.getField(idxStat[j]), colTypes[idxStat[j]]);
			}
		}

		if (intervals != null) {
			for (int j = 0; j < nStat; j++) {
				if (intervals[j] != null) {
					int t = intervals[j].Position(ds[j]);
					intervalCounts[j][t]++;
				}
			}
		}

		if (dotProduction != null) {
			for (int i = 0; i < nStat; i++) {
				for (int j = i; j < nStat; j++) {
					dotProduction[i][j] += ds[i] * ds[j];
				}
			}
		}
	}

	//srt.itvcalc will be calc.
	private static void updateInterVals(Interval[] intervals, long[][] intervalCounts, SummaryResultTable srt) {
		if (intervals != null) {
			for (int j = 0; j < intervals.length; j++) {
				if (intervals[j] != null) {
					srt.src[j].itvcalc = new IntervalCalculator(intervals[j].start, intervals[j].step,
						intervalCounts[j]);
				}
			}
		}
	}

	public static Map <String, SrtForWp> summaryForGroup(String[] colNames, Class[] colTypes, Iterable <Row> data,
														 int[] groupIdxs, String[] statColNames) throws Exception {

		List <String> keys = new ArrayList <>();

		List <MeasureIteratorBase[]> measures = new ArrayList <>();

		if (colNames == null || colNames.length == 0) {
			throw new AkIllegalOperatorParameterException("colNames must not be empty.");
		}
		if (colTypes == null || colTypes.length == 0) {
			throw new AkIllegalOperatorParameterException("colTypes must not be empty.");
		}
		if (colNames.length != colTypes.length) {
			throw new AkIllegalOperatorParameterException("colNames length must equal with colTypes length.");
		}

		int[] idxStat = new int[statColNames.length];
		for (int i = 0; i < statColNames.length; i++) {
			idxStat[i] = TableUtil.findColIndex(colNames, statColNames[i]);
			if (idxStat[i] < 0) {
				throw new AkIllegalOperatorParameterException("stat col not exist.");
			}
		}

		int nStat = idxStat.length;

		for (Row row : data) {
			String keyVal = getStringValue(row, groupIdxs[0]);
			for (int i = 1; i < groupIdxs.length; i++) {
				keyVal = keyVal + "\u0001" + getStringValue(row, groupIdxs[i]);
			}

			int keyIdx = keys.indexOf(keyVal);
			if (keyIdx < 0) {
				keys.add(keyVal);
				MeasureIteratorBase[] mis = new MeasureIteratorBase[nStat];

				for (int j = 0; j < nStat; j++) {
					int index = idxStat[j];
					if (Double.class == colTypes[index] || Float.class == colTypes[index]) {
						mis[j] = new MeasureIteratorDouble(colTypes[index]);
					} else if (Long.class == colTypes[index] || Integer.class == colTypes[index]
						|| Short.class == colTypes[index] || Byte.class == colTypes[index]) {
						mis[j] = new MeasureIteratorLong(colTypes[index]);
					} else if (String.class == colTypes[index]) {
						mis[j] = new MeasureIteratorString(colTypes[index]);
					} else if (Boolean.class == colTypes[index]) {
						mis[j] = new MeasureIteratorBoolean(colTypes[index]);
					} else if (java.sql.Timestamp.class == colTypes[index]) {
						mis[j] = new MeasureIteratorDate(colTypes[index]);
					} else {
						throw new AkUnsupportedOperationException(String.format(
							"col type [%s] not supported.", colTypes[index].getSimpleName()));
					}
				}
				for (int i = 0; i < nStat; i++) {
					Object obj = row.getField(idxStat[i]);
					mis[i].visit(obj);
				}
				measures.add(mis);
			} else {
				MeasureIteratorBase[] mis = measures.get(keyIdx);
				for (int i = 0; i < nStat; i++) {
					Object obj = row.getField(idxStat[i]);
					mis[i].visit(obj);
				}
			}
		}

		Map <String, SrtForWp> srtMaps = new HashMap <>();
		for (int i = 0; i < keys.size(); i++) {
			SrtForWp srt = new SrtForWp(statColNames);
			String key = keys.get(i);
			MeasureIteratorBase[] mis = measures.get(i);
			for (int j = 0; j < nStat; j++) {
				mis[j].finalResult(srt.src[j]);
			}
			srtMaps.put(key, srt);
		}

		return srtMaps;
	}

	public static String getStringValue(Row row, int idx) {
		Object keyObj = row.getField(idx);
		if (keyObj == null) {
			return "null";
		} else {
			return keyObj.toString();
		}
	}

}

class MeasureIteratorString extends MeasureIteratorBase {

	public MeasureIteratorString(Class dataType) {
		super(dataType);
	}

	@Override
	public void visit(Object obj) {
		if (null == obj) {
			countMissing++;
		} else {
			count++;
		}
	}

	@Override
	public void finalResult(SummaryResultCol src) {
		src.init(dataType, count + countMissing, count, countMissing, 0, 0, 0, 0,
			0, 0, 0, 0, null, null);
	}
}

class MeasureIteratorBoolean extends MeasureIteratorBase {

	long countTrue;
	long countFalse;

	public MeasureIteratorBoolean(Class dataType) {
		super(dataType);
		countTrue = 0;
		countFalse = 0;
	}

	@Override
	public void visit(Object obj) {
		if (null == obj) {
			countMissing++;
		} else {
			if (obj.equals(Boolean.TRUE)) {
				countTrue++;
			} else {
				countFalse++;
			}
		}
	}

	@Override
	public void finalResult(SummaryResultCol src) {
		count = countTrue + countFalse;
		Boolean min = null;
		Boolean max = null;
		if (count > 0) {
			min = Boolean.FALSE;
			max = Boolean.TRUE;
			if (0 == countTrue) {
				max = Boolean.FALSE;
			}
			if (0 == countFalse) {
				min = Boolean.TRUE;
			}
		}
		src.init(dataType, count + countMissing, count, countMissing, 0, 0, 0,
			countTrue, countTrue, countTrue, countTrue, countTrue, min, max);
	}
}

class MeasureIteratorLong extends MeasureIteratorBase {
	public double sum;
	public long minLong;
	public long maxLong;
	public Number min = null;
	public Number max = null;
	double sum2;
	double sum3;
	double sum4;
	double absSum;

	public MeasureIteratorLong(Class dataType) {
		super(dataType);
		sum = 0.0;
		sum2 = 0.0;
		sum3 = 0.0;
		sum4 = 0.0;
		minLong = Long.MAX_VALUE;
		maxLong = Long.MIN_VALUE;
		absSum = 0;
	}

	@Override
	public void visit(Object obj) {
		if (null == obj) {
			countMissing++;
		} else {
			long d = ((Number) obj).longValue();
			sum += d;
			absSum += Math.abs(d);
			sum2 += d * d;
			sum3 += d * d * d;
			sum4 += d * d * d * d;
			count++;
			if (d > maxLong) {
				maxLong = d;
				max = (Number) obj;
			}
			if (d < minLong) {
				minLong = d;
				min = (Number) obj;
			}
		}
	}

	@Override
	public void finalResult(SummaryResultCol src) {
		src.init(dataType, count + countMissing, count, countMissing, 0, 0, 0,
			sum, absSum, sum2, sum3, sum4, min, max);
	}
}

class MeasureIteratorDate extends MeasureIteratorBase {

	public long minLong;
	public long maxLong;

	public MeasureIteratorDate(Class dataType) {
		super(dataType);
		minLong = Long.MAX_VALUE;
		maxLong = Long.MIN_VALUE;
	}

	@Override
	public void visit(Object obj) {
		if (null == obj) {
			countMissing++;
		} else {
			long d = ((java.sql.Timestamp) obj).getTime();
			count++;
			if (d > maxLong) {
				maxLong = d;
			}
			if (d < minLong) {
				minLong = d;
			}
		}

	}

	@Override
	public void finalResult(SummaryResultCol src) {
		src.init(dataType,
			count + countMissing, count, countMissing,
			0, 0, 0, 0, 0, 0, 0, 0,
			minLong, maxLong);
	}
}

class MeasureIteratorDouble extends MeasureIteratorBase {

	public double sum;
	public double minDouble;
	public double maxDouble;
	public Number min = null;
	public Number max = null;
	double absSum;
	double sum2;
	double sum3;
	double sum4;

	public MeasureIteratorDouble(Class dataType) {
		super(dataType);
		sum = 0.0;
		sum2 = 0.0;
		sum3 = 0.0;
		sum4 = 0.0;
		absSum = 0;
		minDouble = Double.POSITIVE_INFINITY;
		maxDouble = Double.NEGATIVE_INFINITY;
	}

	@Override
	public void visit(Object obj) {
		if (null == obj) {
			countMissing++;
		} else {
			double d = ((Number) obj).doubleValue();
			sum += d;
			absSum += Math.abs(d);
			sum2 += d * d;
			sum3 += d * d * d;
			sum4 += d * d * d * d;
			count++;
			if (d > maxDouble) {
				maxDouble = d;
				max = (Number) obj;
			}
			if (d < minDouble) {
				minDouble = d;
				min = (Number) obj;
			}
		}
	}

	@Override
	public void finalResult(SummaryResultCol src) {
		src.init(dataType, count + countMissing, count, countMissing, 0, 0, 0,
			sum, absSum, sum2, sum3, sum4, min, max);
	}
}

abstract class MeasureIteratorBase {

	Class dataType;
	long count;
	long countMissing;

	public MeasureIteratorBase(Class dataType) {
		this.dataType = dataType;
		count = 0;
		countMissing = 0;
	}

	abstract void visit(Object obj);

	abstract void finalResult(SummaryResultCol src);
}

class FrequencyIterator {

	int capacity;
	Class dataType;
	TreeMap <Object, Long> mapFreq = null;
	boolean bOutOfRange;

	public FrequencyIterator(Class dataType, int capacity) {
		this.capacity = capacity;
		this.dataType = dataType;
		this.mapFreq = new TreeMap <Object, Long>();
		this.bOutOfRange = false;
	}

	public void visit(Object o) {
		if (bOutOfRange || o == null) {
			return;
		}
		if (o.getClass() == this.dataType) {
			if (this.mapFreq.containsKey(o)) {
				this.mapFreq.put(o, this.mapFreq.get(o) + 1);
			} else {
				if (mapFreq.size() >= this.capacity) {
					this.bOutOfRange = true;
					this.mapFreq.clear();
				} else {
					this.mapFreq.put(o, new Long(1));
				}
			}
		} else {
			throw new AkIllegalStateException("Not valid class type!");
		}
	}

	public void finalResult(SummaryResultCol src) {
		if (this.bOutOfRange) {
			src.freq = null;
		} else {
			src.freq = new TreeMap <Object, Long>();
			Iterator <Map.Entry <Object, Long>> it = this.mapFreq.entrySet().iterator();
			if (dataType == java.sql.Timestamp.class) {
				while (it.hasNext()) {
					Map.Entry <Object, Long> e = it.next();
					src.freq.put(((java.sql.Timestamp) e.getKey()).getTime(), e.getValue());
				}
			} else {
				while (it.hasNext()) {
					Map.Entry <Object, Long> e = it.next();
					src.freq.put(e.getKey(), e.getValue());
				}
			}
		}
	}

}

class DistinctValueIterator {
	public HashSet <Object> mapFreq = null;
	Class dataType;

	public DistinctValueIterator(Class dataType) {
		this.dataType = dataType;
		this.mapFreq = new HashSet <>();
	}

	public void visit(Object o) {
		if (o.getClass() == this.dataType) {
			mapFreq.add(o);
		} else {
			throw new AkIllegalStateException("Not valid class type!");
		}
	}
}

class TopKInterator {

	Class dataType;
	int smallK;
	int largeK;
	PriorityQueue <Object> priqueS;
	PriorityQueue <Object> priqueL;

	public TopKInterator(Class dataType, int small, int large) {
		this.dataType = dataType;
		this.smallK = small;
		this.largeK = large;
		priqueS = new PriorityQueue <>(small, new ValueComparator(this.dataType, -1));
		priqueL = new PriorityQueue <>(large, new ValueComparator(this.dataType, 1));
	}

	public void visit(Object obj) {
		if (obj == null) {
			return;
		}
		if (priqueL.size() < largeK) {
			priqueL.add(obj);
		} else {
			priqueL.add(obj);
			priqueL.poll();
		}
		if (priqueS.size() < smallK) {
			priqueS.add(obj);
		} else {
			priqueS.add(obj);
			priqueS.poll();
		}
	}

	public void finalResult(SummaryResultCol src) {
		int large = priqueL.size();
		int small = priqueS.size();
		src.topItems = new Object[large];
		src.bottomItems = new Object[small];
		if (dataType == java.sql.Timestamp.class) {
			for (int i = 0; i < small; i++) {
				src.bottomItems[small - i - 1] = ((java.sql.Timestamp) priqueS.poll()).getTime();
			}
			for (int i = 0; i < large; i++) {
				src.topItems[large - i - 1] = ((java.sql.Timestamp) priqueL.poll()).getTime();
			}
		} else {
			for (int i = 0; i < small; i++) {
				src.bottomItems[small - i - 1] = priqueS.poll();
			}
			for (int i = 0; i < large; i++) {
				src.topItems[large - i - 1] = priqueL.poll();
			}
		}
	}

	class ValueComparator implements Comparator {

		Class dataType;
		int sortKey = 1;

		ValueComparator(Class dataType, int sortKey) {
			this.dataType = dataType;
			this.sortKey = sortKey;
		}

		@Override
		public int compare(Object t, Object t1) {
			if (Double.class == this.dataType) {
				return (sortKey) * (((Double) t).compareTo((Double) t1));
			} else if (Integer.class == this.dataType) {
				return (sortKey) * (((Integer) t).compareTo((Integer) t1));
			} else if (Long.class == this.dataType) {
				return (sortKey) * (((Long) t).compareTo((Long) t1));
			} else if (Float.class == this.dataType) {
				return (sortKey) * (((Float) t).compareTo((Float) t1));
			} else if (Boolean.class == this.dataType) {
				return (sortKey) * (((Boolean) t).compareTo((Boolean) t1));
			} else if (String.class == this.dataType) {
				return (sortKey) * (((String) t).compareTo((String) t1));
			} else if (java.sql.Timestamp.class == this.dataType) {
				return (sortKey) * (((java.sql.Timestamp) t).compareTo((java.sql.Timestamp) t1));
			} else {
				throw new AkIllegalStateException(String.format(
					"type [%s] not support.", this.dataType.getSimpleName()));
			}
		}

	}

}
