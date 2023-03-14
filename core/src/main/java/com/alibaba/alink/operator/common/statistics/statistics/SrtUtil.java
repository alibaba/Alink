/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.operator.common.statistics.statistics;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticUtil;
import com.alibaba.alink.params.statistics.HasStatLevel_L1;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author yangxu
 */
public class SrtUtil {

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

		BaseMeasureIterator[] mis = newMeasures(wt.colTypes, idxStat);
		FrequencyIterator[] fis = newFreqs(wt.colTypes, idxStat, bCalcFreq, freqSize);
		TopKIterator[] tis = newTopK(wt.colTypes, idxStat, bCalcFreq, smallK, largeK);
		srt.dotProduction = newDotProduction(nStat, bCov);

		Row val;
		Iterator <Row> iter = wt.getIterator();
		while (iter.hasNext()) {
			val = iter.next();
			for (int i = 0; i < nStat; i++) {
				Object obj = val.getField(idxStat[i]);
				mis[i].visit(obj);
				if (tis != null && tis[i] != null) {
					tis[i].visit((Comparable) obj);
				}
				if (fis != null && fis[i] != null) {
					fis[i].visit(obj);
				}
			}
		}

		for (int i = 0; i < nStat; i++) {
			mis[i].finalResult(srt.src[i]);
			srt.src[i].dataType = wt.colTypes[idxStat[i]];
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

		BaseMeasureIterator[] mis = newMeasures(colTypes, idxStat);

		DistinctValueIterator[] fis = new DistinctValueIterator[nStat];
		for (int j = 0; j < nStat; j++) {
			if (needFreqs[j]) {
				fis[j] = StatisticsIteratorFactory.getDistinctValueIterator(colTypes[idxStat[j]]);
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
			srt.src[i].dataType = colTypes[idxStat[i]];
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

	private static BaseMeasureIterator[] newMeasures(Class[] colTypes, int[] idxStat) {
		int nStat = idxStat.length;
		BaseMeasureIterator[] mis = new BaseMeasureIterator[nStat];
		for (int j = 0; j < nStat; j++) {
			int index = idxStat[j];
			mis[j] = StatisticsIteratorFactory.getMeasureIterator(colTypes[index]);
			//if (Double.class == colTypes[index] || Float.class == colTypes[index]) {
			//	mis[j] = new MeasureIteratorDouble(colTypes[index]);
			//} else if (Long.class == colTypes[index] || Integer.class == colTypes[index]
			//	|| Short.class == colTypes[index] || Byte.class == colTypes[index]) {
			//	mis[j] = new MeasureIteratorLong(colTypes[index]);
			//} else if (String.class == colTypes[index]) {
			//	mis[j] = new MeasureIteratorString(colTypes[index]);
			//} else if (Boolean.class == colTypes[index]) {
			//	mis[j] = new MeasureIteratorBoolean(colTypes[index]);
			//} else if (java.sql.Timestamp.class == colTypes[index]) {
			//	mis[j] = new MeasureIteratorDate(colTypes[index]);
			//} else {
			//	throw new AkUnsupportedOperationException(String.format(
			//		"col type [%s] not supported.", colTypes[index].getSimpleName()));
			//}
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
				fis[j] = StatisticsIteratorFactory.getFrequencyIterator(colTypes[index], freqSize);
			}

		}
		return fis;
	}

	private static TopKIterator[] newTopK(Class[] colTypes, int[] idxStat, boolean bCalcFreq, int smallK, int
		largeK) {
		TopKIterator[] fis = null;
		if (bCalcFreq) {
			int nStat = idxStat.length;
			fis = new TopKIterator[nStat];
			for (int j = 0; j < nStat; j++) {
				int index = idxStat[j];
				fis[j] = StatisticsIteratorFactory.getTopKIterator(colTypes[index], smallK, largeK);
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

		List <BaseMeasureIterator[]> measures = new ArrayList <>();

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
				BaseMeasureIterator[] mis = new BaseMeasureIterator[nStat];

				for (int j = 0; j < nStat; j++) {
					int index = idxStat[j];
					mis[j] = StatisticsIteratorFactory.getMeasureIterator(colTypes[index]);
				}
				for (int i = 0; i < nStat; i++) {
					Object obj = row.getField(idxStat[i]);
					mis[i].visit(obj);
				}
				measures.add(mis);
			} else {
				BaseMeasureIterator[] mis = measures.get(keyIdx);
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
			BaseMeasureIterator[] mis = measures.get(i);
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
