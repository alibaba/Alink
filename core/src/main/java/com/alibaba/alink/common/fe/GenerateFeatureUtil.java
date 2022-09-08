package com.alibaba.alink.common.fe;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.fe.define.BaseCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.BaseCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.BaseNumericStatFeatures;
import com.alibaba.alink.common.fe.define.BaseStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceHopWindowStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceLatestStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceNStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceSessionWindowStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceSlotWindowStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceTimeIntervalStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceTimeSlotStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceTumbleWindowStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceWindowStatFeatures;
import com.alibaba.alink.common.fe.define.day.BaseDaysStatFeatures;
import com.alibaba.alink.common.fe.define.day.CategoricalDaysStatistics;
import com.alibaba.alink.common.fe.define.day.NDaysCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.day.NDaysNumericStatFeatures;
import com.alibaba.alink.common.fe.define.day.NumericDaysStatistics;
import com.alibaba.alink.common.fe.define.statistics.BaseCategoricalStatistics;
import com.alibaba.alink.common.fe.define.statistics.BaseNumericStatistics;
import com.alibaba.alink.common.fe.define.statistics.CategoricalStatistics;
import com.alibaba.alink.common.fe.define.statistics.CategoricalStatistics.KvStat;
import com.alibaba.alink.common.fe.define.statistics.CategoricalStatistics.RankType;
import com.alibaba.alink.common.fe.define.statistics.NumericStatistics;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.GenerateFeatureOfLatestNDaysBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.google.common.base.Joiner;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

public class GenerateFeatureUtil {
	public static final String TEMP_MTABLE_COL = "alink_group_temp_mtable_col";
	public static final String[] WINDOW_STAT_OUT_COL_NAME = new String[] {"group_col", "stat_type", "startTime",
		"endTime",
		"stat_val"};
	public static final TypeInformation <?>[] WINDOW_STAT_OUT_COL_TYPES = new TypeInformation <?>[] {
		Types.STRING,
		Types.STRING,
		Types.SQL_TIMESTAMP,
		Types.SQL_TIMESTAMP,
		Types.STRING};

	public static int findStartIdx(MTable mt, String timeCol, BaseStatFeatures <?> feature, Timestamp ts, int idx) {
		if (feature instanceof InterfaceTimeIntervalStatFeatures) {
			return findMtIdx(mt, timeCol,
				getStartTime(ts, ((InterfaceTimeIntervalStatFeatures) feature).getTimeIntervals()[0],
					false));
		} else if (feature instanceof InterfaceTimeSlotStatFeatures) {
			return findMtIdx(mt, timeCol,
				getStartTime(ts, ((InterfaceTimeSlotStatFeatures) feature).getTimeSlots()[0],
					true));
		} else if (feature instanceof InterfaceNStatFeatures) {
			return idx - ((InterfaceNStatFeatures) feature).getNumbers()[0] + 1;
		} else if (feature instanceof InterfaceSlotWindowStatFeatures) {
			return findMtIdx(mt, timeCol,
				getStartTime(ts, ((InterfaceSlotWindowStatFeatures) feature).getWindowTimes()[0],
					true));
		} else {
			throw new AkUnsupportedOperationException(String.format("It is not support yet."));
		}
	}

	//find ts idx in mt.
	public static int findMtIdx(MTable mt, String timeCol, Timestamp ts) {
		return findMtIdx(mt, timeCol, ts, 0);
	}

	public static int findMtIdx(MTable mt, String timeCol, Timestamp ts, int fromId) {
		int idx = 0;
		int timeIdx = TableUtil.findColIndex(mt.getColNames(), timeCol);
		long tsT = ts.getTime();
		for (int i = fromId; i < mt.getNumRow(); i++) {
			if (tsT <= ((Timestamp) mt.getEntry(i, timeIdx)).getTime()) {
				idx = i;
				return idx;
			}
		}
		return mt.getNumRow();
	}

	/**
	 * @return <fromId, endId, windowStartTime, windowEndTime>
	 */
	public static List <Tuple4 <Integer, Integer, Timestamp, Timestamp>> findMtIndices(MTable mt, String timeCol,
																					   InterfaceWindowStatFeatures feature) {
		int timeIdx = TableUtil.findColIndex(mt.getColNames(), timeCol);
		int rowNum = mt.getNumRow();
		Timestamp firstTime = (Timestamp) mt.getEntry(0, timeIdx);
		Timestamp lastTime = (Timestamp) mt.getEntry(rowNum - 1, timeIdx);

		List <Tuple4 <Integer, Integer, Timestamp, Timestamp>> list = new ArrayList <>();

		if (feature instanceof InterfaceHopWindowStatFeatures) {
			String windowTime = ((InterfaceHopWindowStatFeatures) feature).getWindowTimes()[0];
			String hopTime = ((InterfaceHopWindowStatFeatures) feature).getHopTimes()[0];

			int ti = getTimeUnit(windowTime).f1;
			String unit = getTimeUnit(windowTime).f0;
			int hopTi = getTimeUnit(hopTime).f1;
			String hopUnit = getTimeUnit(hopTime).f0;

			if (!unit.equals(hopUnit)) {
				throw new AkIllegalOperatorParameterException("hop window and time window unit must be same.");
			}

			Timestamp startTime = getStartTime(firstTime, unit, ti);
			Timestamp endTime = getNextTime(startTime, unit, ti + hopTi);
			int startIdx = 0;
			int endIdx = findMtIdx(mt, timeCol, endTime);
			while (endTime.getTime() <= lastTime.getTime()) {
				list.add(Tuple4.of(startIdx, endIdx, startTime, endTime));
				startTime = getNextTime(startTime, unit, hopTi);
				endTime = getNextTime(endTime, unit, hopTi);
				startIdx = findMtIdx(mt, timeCol, startTime, startIdx);
				endIdx = findMtIdx(mt, timeCol, endTime, startIdx);
			}
			list.add(Tuple4.of(startIdx, endIdx, startTime, endTime));
		} else if (feature instanceof InterfaceSessionWindowStatFeatures) {
			String sessionTime = ((InterfaceSessionWindowStatFeatures) feature).getSessionGapTimes()[0];
			int ti = getTimeUnit(sessionTime).f1;
			String unit = getTimeUnit(sessionTime).f0;
			Timestamp startTime = firstTime;
			int startIdx = 0;

			Timestamp curTs = firstTime;
			for (int i = 1; i < rowNum; i++) {
				Timestamp nextTs = getNextTime(startTime, unit, ti);
				curTs = (Timestamp) mt.getEntry(i, timeIdx);
				if (curTs.after(nextTs)) {
					list.add(Tuple4.of(startIdx, i, startTime, curTs));
					startIdx = i;
				}
				startTime = curTs;
			}
			list.add(Tuple4.of(startIdx, rowNum, startTime, curTs));
		} else if (feature instanceof InterfaceTumbleWindowStatFeatures) {
			String windowTime = ((InterfaceTumbleWindowStatFeatures) feature).getWindowTimes()[0];
			int ti = getTimeUnit(windowTime).f1;
			String unit = getTimeUnit(windowTime).f0;

			Timestamp startTime = getStartTime(firstTime, unit, ti);
			Timestamp endTime = getNextTime(startTime, unit, ti);

			int startIdx = 0;
			int endIdx = findMtIdx(mt, timeCol, endTime);
			while (endTime.getTime() <= lastTime.getTime()) {
				list.add(Tuple4.of(startIdx, endIdx, startTime, endTime));
				startTime = endTime;
				startIdx = endIdx;
				endTime = getNextTime(startTime, unit, ti);
				endIdx = findMtIdx(mt, timeCol, endTime, startIdx);
			}
			list.add(Tuple4.of(startIdx, endIdx, startTime, endTime));
		} else if (feature instanceof InterfaceSlotWindowStatFeatures) {
			String windowTime = ((InterfaceSlotWindowStatFeatures) feature).getWindowTimes()[0];
			String stepTime = ((InterfaceSlotWindowStatFeatures) feature).getStepTimes()[0];

			int ti = getTimeUnit(windowTime).f1;
			String unit = getTimeUnit(windowTime).f0;
			int stepTi = getTimeUnit(stepTime).f1;
			String stepUnit = getTimeUnit(stepTime).f0;

			if (!unit.equals(stepUnit)) {
				throw new AkIllegalOperatorParameterException("hop window and time window unit must be same.");
			}

			Timestamp startTime = getStartTime(firstTime, windowTime, true);
			Timestamp endTime = getNextTime(startTime, unit, stepTi);
			int startIdx = 0;
			int endIdx = findMtIdx(mt, timeCol, endTime);
			while (endTime.getTime() <= lastTime.getTime()) {
				list.add(Tuple4.of(startIdx, endIdx, startTime, endTime));
				endTime = getNextTime(endTime, stepUnit, stepTi);
				Timestamp startTimeTmp = getNextTime(startTime, unit, ti);
				if (endTime.getTime() > startTimeTmp.getTime()) {
					startTime = startTimeTmp;
				}

				startIdx = findMtIdx(mt, timeCol, startTime, startIdx);
				endIdx = findMtIdx(mt, timeCol, endTime, startIdx);

			}
			list.add(Tuple4.of(startIdx, endIdx, startTime, endTime));
		}
		return list;
	}

	private static Timestamp getLastSlotTime(Timestamp ts, long interval) {
		if (ts.getTime() % interval == 0) {
			return new Timestamp(ts.getTime() - interval);
		} else {
			return new Timestamp(ts.getTime() / interval * interval);
		}
	}

	//find window start time.  support y, d, M, h, m, s.
	public static Timestamp getStartTime(Timestamp endTime, String timeInterval, boolean isSlot) {
		int ti = Integer.parseInt(timeInterval.substring(0, timeInterval.length() - 1));
		String unit = timeInterval.substring(timeInterval.length() - 1);
		LocalDateTime localEndTime = endTime.toLocalDateTime();
		LocalDateTime localStartTime = null;
		if (isSlot) {
			long interval = 0;
			switch (unit) {
				case "s":
					interval = ti * 1000L;
					return getLastSlotTime(endTime, interval);
				case "m":
					interval = ti * 60_000L;
					return getLastSlotTime(endTime, interval);
				case "h":
					interval = ti * 3600_000L;
					return getLastSlotTime(endTime, interval);
				case "d":
					interval = ti * 43200_000L;
					return getLastSlotTime(endTime, interval);
				case "M":
					localStartTime = localEndTime
						.withMonth(ti - 1)
						.withDayOfMonth(1)
						.withMinute(0)
						.withSecond(0)
						.withNano(0);
					break;
				case "y":
					localStartTime = localEndTime
						.minusYears(ti - 1)
						.withMonth(1)
						.withDayOfMonth(1)
						.withMinute(0)
						.withSecond(0)
						.withNano(0);
					break;
				default:
					throw new AkUnsupportedOperationException(
						String.format("unit [%s] not support yet.", unit));
			}
		} else {
			switch (unit) {
				case "s":
					localStartTime = localEndTime.minusSeconds(ti);
					break;
				case "m":
					localStartTime = localEndTime.minusMinutes(ti);
					break;
				case "h":
					localStartTime = localEndTime.minusHours(ti);
					break;
				case "M":
					localStartTime = localEndTime.minusMonths(ti);
					break;
				case "d":
					localStartTime = localEndTime.minusDays(ti);
					break;
				case "y":
					localStartTime = localEndTime.minusYears(ti);
					break;
				default:
					throw new AkUnsupportedOperationException(
						String.format("unit [%s] not support yet.", unit));
			}
		}
		return Timestamp.valueOf(localStartTime);

	}

	public static void calStatistics(MTable mt, Tuple4 <Integer, Integer, Timestamp, Timestamp> ins, String timeCol,
									 BaseStatFeatures <?> feature,
									 Object[] out) {
		if (feature instanceof BaseNumericStatFeatures) {
			calNumericStatistics(mt, ins, timeCol,
				(BaseNumericStatFeatures <?>) feature,
				out);
		} else if (feature instanceof BaseCategoricalStatFeatures) {
			calCategoryStatistics(mt, ins, timeCol,
				(BaseCategoricalStatFeatures <?>) feature, out);
		} else if (feature instanceof BaseCrossCategoricalStatFeatures) {
			calCrossCategoryStatistics(mt, ins, timeCol,
				(BaseCrossCategoricalStatFeatures <?>) feature, out);
		}
	}

	static void calNumericStatistics(MTable mt, Tuple4 <Integer, Integer, Timestamp, Timestamp> ins, String timeCol,
									 BaseNumericStatFeatures feature,
									 Object[] out) {
		int fromId = Math.max(0, ins.f0);
		int endId = ins.f1;
		Timestamp startTime = ins.f2;
		Timestamp endTime = ins.f3;

		TableSummary summary = mt.subSummary(feature.featureCols, fromId, endId);
		int rowNum = mt.getNumRow();
		double EPS = 1e-16;
		int timeIdx = TableUtil.findColIndex(mt.getColNames(), timeCol);

		int idx = 0;
		for (String featureCol : feature.featureCols) {
			for (BaseNumericStatistics type : feature.types) {
				if (type instanceof NumericStatistics.FirstTimeN) {
					int n = ((NumericStatistics.FirstTimeN) type).getTimeN();
					Timestamp t = (Timestamp) mt.getEntry(fromId, timeIdx);
					int count = 1;
					for (int i = fromId + 1; i < endId; i++) {
						if (count == n) {
							break;
						}
						Timestamp nextT = (Timestamp) mt.getEntry(i, timeIdx);
						count = nextT.equals(t) ? count : count + 1;
						t = nextT;
					}
					out[idx++] = count == n ? t : null;
				} else if (type instanceof NumericStatistics.LastTimeN) {
					int n = ((NumericStatistics.LastTimeN) type).getTimeN();
					Timestamp t = (Timestamp) mt.getEntry(endId - 1, timeIdx);
					int count = 1;
					for (int i = endId - 2; i >= fromId; i--) {
						if (count == n) {
							break;
						}
						Timestamp nextT = (Timestamp) mt.getEntry(i, timeIdx);
						count = nextT.equals(t) ? count : count + 1;
						t = nextT;
					}
					out[idx++] = count == n ? t : null;
				} else if (NumericStatistics.RANGE_START_TIME.equals(type)) {
					out[idx++] = startTime;
				} else if (NumericStatistics.RANGE_END_TIME.equals(type)) {
					out[idx++] = endTime;
				} else if (NumericStatistics.COUNT.equals(type)) {
					out[idx++] = summary.numValidValue(featureCol);
				} else if (NumericStatistics.TOTAL_COUNT.equals(type)) {
					out[idx++] = endId - fromId;
				} else if (NumericStatistics.SUM.equals(type)) {
					out[idx++] = summary.sum(featureCol);
				} else if (NumericStatistics.MEAN.equals(type)) {
					out[idx++] = summary.mean(featureCol);
				} else if (NumericStatistics.STDDEV_SAMP.equals(type)) {
					double square_sum = Math.pow(summary.normL2(featureCol), 2);
					double sum = summary.sum(featureCol);
					double mean = summary.mean(featureCol);
					long count = endId - fromId;
					if (count == 1) {
						out[idx++] = 0.;
					} else {
						out[idx++] = Math.sqrt(
							(square_sum - 2 * mean * sum) / (count - 1) + (mean * mean * count) / (count - 1));
					}
				} else if (NumericStatistics.STDDEV_POP.equals(type)) {
					double square_sum = Math.pow(summary.normL2(featureCol), 2);
					double sum = summary.sum(featureCol);
					double mean = summary.mean(featureCol);
					long count = endId - fromId;
					out[idx++] = Math.sqrt((square_sum - 2 * mean * sum) / count + mean * mean);
				} else if (NumericStatistics.VAR_SAMP.equals(type)) {
					double square_sum = Math.pow(summary.normL2(featureCol), 2);
					double sum = summary.sum(featureCol);
					double mean = summary.mean(featureCol);
					long count = endId - fromId;
					if (count == 1) {
						out[idx++] = 0.;
					} else {
						out[idx++] = (square_sum - 2 * mean * sum) / (count - 1) + (mean * mean * count) / (count - 1);
					}
				} else if (NumericStatistics.VAR_POP.equals(type)) {
					double square_sum = Math.pow(summary.normL2(featureCol), 2);
					double sum = summary.sum(featureCol);
					double mean = summary.mean(featureCol);
					long count = endId - fromId;
					out[idx++] = (square_sum - 2 * mean * sum) / count + mean * mean;
				} else if (NumericStatistics.SKEWNESS.equals(type)) {
					double square_sum = Math.pow(summary.normL2(featureCol), 2);
					double sum = summary.sum(featureCol);
					double mean = summary.mean(featureCol);
					long count = endId - fromId;
					if (count == 1) {
						out[idx++] = 0.;
					} else {
						double var = (square_sum - 2 * mean * sum) / (count - 1) + (mean * mean * count) / (count - 1);
						out[idx++] = summary.centralMoment3(featureCol) / Math.pow(var, 1.5);
					}
				} else if (NumericStatistics.SQUARE_SUM.equals(type)) {
					out[idx++] = Math.pow(summary.normL2(featureCol), 2);
				} else if (NumericStatistics.MEDIAN.equals(type)) {
					int count = endId - fromId;
					double[] values = new double[count];
					int featureColIndex = TableUtil.findColIndex(mt.getColNames(), featureCol);
					for (int i = fromId; i < endId; i++) {
						values[i - fromId] = ((Number) mt.getEntry(i, featureColIndex)).doubleValue();
					}
					out[idx++] = findMidValue(values);
				} else if (NumericStatistics.MODE.equals(type)) {
					int count = endId - fromId;
					double[] values = new double[count];
					int featureColIndex = TableUtil.findColIndex(mt.getColNames(), featureCol);
					for (int i = fromId; i < endId; i++) {
						values[i - fromId] = ((Number) mt.getEntry(i, featureColIndex)).doubleValue();
					}
					out[idx++] = findModeValue(values);
				} else if (NumericStatistics.FREQ.equals(type)) {
					int freq = 0;
					int featureColIndex = TableUtil.findColIndex(mt.getColNames(), featureCol);
					double value = ((Number) mt.getEntry(endId - 1, featureColIndex)).doubleValue();
					for (int i = fromId; i < endId; i++) {
						if (value == ((Number) mt.getEntry(i, featureColIndex)).doubleValue()) {
							freq++;
						}
					}
					out[idx++] = freq;
				} else if (type instanceof NumericStatistics.LastN) {
					int n = ((NumericStatistics.LastN) type).getN();
					if (endId - n < 0) {
						out[idx++] = null;
					} else {
						out[idx++] = mt.getEntry(endId - n, TableUtil.findColIndex(mt.getColNames(), featureCol));
					}
				} else if (type instanceof NumericStatistics.FirstN) {
					int n = ((NumericStatistics.FirstN) type).getN();
					if (fromId + n >= rowNum) {
						out[idx++] = null;
					} else {
						out[idx++] = mt.getEntry(fromId + n, TableUtil.findColIndex(mt.getColNames(), featureCol));
					}
				} else if (NumericStatistics.RANK.equals(type)) {
					int rank = 1;
					int featureColIndex = TableUtil.findColIndex(mt.getColNames(), featureCol);
					Double thisValue = ((Number) mt.getEntry(endId - 1, featureColIndex)).doubleValue();
					for (int i = endId - 2; i >= fromId; i--) {
						Double value = ((Number) mt.getEntry(i, featureColIndex)).doubleValue();
						if (value < thisValue) {rank++;}
					}
					out[idx++] = rank;
				} else if (NumericStatistics.DENSE_RANK.equals(type)) {
					Set <Double> valueFreqMap = new HashSet <>();
					int rank = 1;
					int featureColIndex = TableUtil.findColIndex(mt.getColNames(), featureCol);
					Double thisValue = ((Number) mt.getEntry(endId - 1, featureColIndex)).doubleValue();
					for (int i = endId - 2; i >= fromId; i--) {
						Double value = ((Number) mt.getEntry(i, featureColIndex)).doubleValue();
						if (value < thisValue && !valueFreqMap.contains(value)) {
							rank++;
							valueFreqMap.add(value);
						}
					}
					out[idx++] = rank;
				} else if (type instanceof NumericStatistics.ConcatAgg) {
					out[idx++] = concatAggOneColumn(mt, fromId, endId, featureCol,
						((NumericStatistics.ConcatAgg) type).getDelimiter());
				} else if (type instanceof NumericStatistics.SumLastN) {
					int n = ((NumericStatistics.SumLastN) type).getN();
					int featureColIndex = TableUtil.findColIndex(mt.getColNames(), featureCol);
					n = endId - n < 0 ? 0 : endId - n;
					double lastNSum = 0;
					for (int i = n; i < endId; i++) {
						lastNSum += ((Number) mt.getEntry(i, featureColIndex)).doubleValue();
					}
				} else {
					throw new AkUnsupportedOperationException("It is not support yet." + type);
				}
			}
		}
	}

	static double findMidValue(double[] values) {
		int n = values.length;
		int mid = n / 2;
		int left = 0;
		int right = n - 1;
		int p = left;
		int q = right;
		while (true) {
			while (values[q] >= values[left] && q > left) {
				q--;
			}
			while (values[p] <= values[left] && p < q) {
				p++;
			}
			if (p < q) {
				double tmp = values[q];
				values[q] = values[p];
				values[p] = tmp;
			}
			if (p == q) {
				double tmp = values[left];
				values[left] = values[p];
				values[p] = tmp;
				if (p == mid) {
					break;
				} else if (p < mid) {
					left = p + 1;
				} else {
					right = p - 1;
				}
				p = left;
				q = right;
			}
		}
		if (n % 2 == 0) {
			left = Math.max(left - 1, 0);
			double max = values[left];
			for (int i = left + 1; i < p; i++) {
				max = Math.max(values[i], max);
			}
			return (values[mid] + max) / 2;
		} else {
			return values[mid];
		}
	}

	static double findModeValue(double[] values) {
		long maxFreq = 0;
		double mode = values[0];
		HashMap <Double, Long> freqMap = new HashMap <>();
		for (double value : values) {
			if (freqMap.isEmpty()) {
				freqMap.put(value, 1L);
				mode = value;
				continue;
			}
			if (freqMap.containsKey(value)) {
				Long freq = freqMap.get(value) + 1;
				freqMap.put(value, freq);
				if (freq > maxFreq) {
					maxFreq = freq;
					mode = value;
				}
			} else {
				freqMap.put(value, 1L);
			}
		}
		return mode;
	}

	static HashMap <List <Object>, Integer> kvCount(MTable mt, int fromId, int endId, String[] featureCols) {
		int[] colIndexes = TableUtil.findColIndices(mt.getSchema(), featureCols);
		HashMap <List <Object>, Integer> counts = new HashMap <>();
		for (int i = fromId; i < endId; i++) {
			List <Object> list = new ArrayList <>(colIndexes.length);
			for (int j = 0; j < colIndexes.length; j++) {
				Object k2 = mt.getEntry(i, colIndexes[j]);
				list.add(k2);
			}
			if (counts.containsKey(list)) {
				counts.put(list, counts.get(list) + 1);
			} else {
				counts.put(list, 1);
			}
		}
		return counts;
	}

	static String concatAggOneColumn(MTable mt, int fromId, int endId, String featureCol, String delimiter) {
		StringBuilder sbd = new StringBuilder();
		int feColIndex = TableUtil.findColIndex(mt.getSchema(), featureCol);
		for (int i = fromId; i < endId; i++) {
			sbd.append(mt.getEntry(i, feColIndex).toString());
			sbd.append(delimiter);
		}
		return sbd.substring(0, sbd.length() - delimiter.length());
	}

	static void calCategoryStatistics(MTable mt, Tuple4 <Integer, Integer, Timestamp, Timestamp> ins, String timeCol,
									  BaseCategoricalStatFeatures <?> feature,
									  Object[] out) {
		int fromId = Math.max(0, ins.f0);
		int endId = ins.f1;
		Timestamp startTime = ins.f2;
		Timestamp endTime = ins.f3;
		TableSummary summary = mt.subSummary(feature.featureCols, fromId, endId);
		int rowNum = mt.getNumRow();
		int timeIdx = TableUtil.findColIndex(mt.getColNames(), timeCol);

		int idx = 0;
		int colIdx = 0;
		for (String featureCol : feature.featureCols) {
			for (BaseCategoricalStatistics type : feature.types) {
				if (type instanceof CategoricalStatistics.FirstTimeN) {
					int n = ((CategoricalStatistics.FirstTimeN) type).getTimeN();
					Timestamp t = (Timestamp) mt.getEntry(fromId, timeIdx);
					int count = 1;
					for (int i = fromId + 1; i < endId; i++) {
						if (count == n) {
							break;
						}
						Timestamp nextT = (Timestamp) mt.getEntry(i, timeIdx);
						count = nextT.equals(t) ? count : count + 1;
						t = nextT;
					}
					out[idx++] = count == n ? t : null;
				} else if (type instanceof CategoricalStatistics.LastTimeN) {
					int n = ((CategoricalStatistics.LastTimeN) type).getTimeN();
					Timestamp t = (Timestamp) mt.getEntry(endId - 1, timeIdx);
					int count = 1;
					for (int i = endId - 2; i >= fromId; i--) {
						if (count == n) {
							break;
						}
						Timestamp nextT = (Timestamp) mt.getEntry(i, timeIdx);
						count = nextT.equals(t) ? count : count + 1;
						t = nextT;
					}
					out[idx++] = count == n ? t : null;
				} else if (CategoricalStatistics.RANGE_START_TIME.equals(type)) {
					out[idx++] = startTime;
				} else if (CategoricalStatistics.RANGE_END_TIME.equals(type)) {
					out[idx++] = endTime;
				} else if (CategoricalStatistics.COUNT.equals(type)) {
					out[idx++] = summary.numValidValue(featureCol);
				} else if (CategoricalStatistics.DISTINCT_COUNT.equals(type)) {
					HashMap <List <Object>, Integer> kvCount = kvCount(mt, fromId, endId, new String[] {featureCol});
					out[idx++] = kvCount.size();
				} else if (CategoricalStatistics.TOTAL_COUNT.equals(type)) {
					out[idx++] = endId - fromId;
				} else if (CategoricalStatistics.RATIO.equals(type)) {
					if (type instanceof InterfaceLatestStatFeatures) {
						HashMap <List <Object>, Integer> kvCount = kvCount(mt, fromId, endId,
							new String[] {featureCol});
						Object obj = mt.getEntry(endId - 1, TableUtil.findColIndex(mt.getColNames(), featureCol));
						out[idx++] = (double) kvCount.get(Arrays.asList(obj)) / (double) summary.numValidValue(
							featureCol);
					} else {
						throw new AkUnsupportedOperationException("ratio is not support yet in " + type);
					}
				} else if (type instanceof CategoricalStatistics.LastN) {
					int n = ((CategoricalStatistics.LastN) type).getN();
					if (endId - n < 0) {
						out[idx++] = null;
					} else {
						int featureIndex = TableUtil.findColIndex(mt.getColNames(), featureCol);
						out[idx++] = mt.getEntry(endId - n, featureIndex);
					}
				} else if (type instanceof CategoricalStatistics.FirstN) {
					int n = ((CategoricalStatistics.FirstN) type).getN();
					if (fromId + n >= rowNum) {
						out[idx++] = null;
					} else {
						int featureIndex = TableUtil.findColIndex(mt.getColNames(), featureCol);
						out[idx++] = mt.getEntry(fromId + n, featureIndex);
					}
				} else if (type instanceof CategoricalStatistics.ConcatAgg) {
					out[idx++] = concatAggOneColumn(mt, fromId, endId, featureCol,
						((CategoricalStatistics.ConcatAgg) type).getDelimiter());
				} else if (CategoricalStatistics.IS_EXIST.equals(type)) {
					int featureIndex = TableUtil.findColIndex(mt.getColNames(), featureCol);
					Object obj = mt.getEntry(endId - 1, featureIndex);
					int i = endId - 2;
					while (!(i < fromId || obj.equals(mt.getEntry(i, featureIndex)))) {
						i--;
					}
					out[idx++] = i >= fromId;
				} else {
					throw new AkUnsupportedOperationException("It is not support yet." + type);
				}
			}
			colIdx++;
		}
	}

	static String rowPartFieldsToString(Row row, int[] colIdxs, String[] featureCols) {
		StringBuilder sbd = new StringBuilder();
		sbd.append("{");
		for (int i = 0; i < colIdxs.length - 1; i++) {
			sbd.append("\"").append(featureCols[i]).append("\":\"");
			sbd.append(row.getField(colIdxs[i])).append("\",");
		}
		sbd.append("\"").append(featureCols[colIdxs.length - 1]).append("\":\"");
		sbd.append(row.getField(colIdxs[colIdxs.length - 1])).append("\"}");
		return sbd.toString();
	}

	static void calCrossCategoryStatistics(MTable mt, Tuple4 <Integer, Integer, Timestamp, Timestamp> ins,
										   String timeCol,
										   BaseCrossCategoricalStatFeatures <?> feature,
										   Object[] out) {
		int fromId = Math.max(0, ins.f0);
		int endId = ins.f1;
		Timestamp startTime = ins.f2;
		Timestamp endTime = ins.f3;

		int rowNum = mt.getNumRow();
		List <Row> rows = mt.getRows();
		int timeIdx = TableUtil.findColIndex(mt.getColNames(), timeCol);

		int idx = 0;
		for (String[] featureCols : feature.crossFeatureCols) {
			for (BaseCategoricalStatistics type : feature.types) {
				long validCount = 0;
				int[] colIndexs = TableUtil.findColIndicesWithAssertAndHint(mt.getColNames(), featureCols);
				for (int i = fromId; i < endId; i++) {
					boolean hasNull = false;
					for (int j : colIndexs) {
						if (mt.getEntry(i, j) == null) {
							hasNull = true;
							break;
						}
					}
					validCount = hasNull ? validCount : validCount + 1;
				}
				if (type instanceof CategoricalStatistics.FirstTimeN) {
					int n = ((CategoricalStatistics.FirstTimeN) type).getTimeN();
					Timestamp t = (Timestamp) mt.getEntry(fromId, timeIdx);
					int count = 1;
					for (int i = fromId + 1; i < endId; i++) {
						if (count == n) {
							break;
						}
						Timestamp nextT = (Timestamp) mt.getEntry(i, timeIdx);
						count = nextT.equals(t) ? count : count + 1;
						t = nextT;
					}
					out[idx++] = count == n ? t : null;
				} else if (type instanceof CategoricalStatistics.LastTimeN) {
					int n = ((CategoricalStatistics.LastTimeN) type).getTimeN();
					Timestamp t = (Timestamp) mt.getEntry(endId - 1, timeIdx);
					int count = 1;
					for (int i = endId - 2; i >= fromId; i--) {
						if (count == n) {
							break;
						}
						Timestamp nextT = (Timestamp) mt.getEntry(i, timeIdx);
						count = nextT.equals(t) ? count : count + 1;
						t = nextT;
					}
					out[idx++] = count == n ? t : null;
				} else if (CategoricalStatistics.RANGE_START_TIME.equals(type)) {
					out[idx++] = startTime;
				} else if (CategoricalStatistics.RANGE_END_TIME.equals(type)) {
					out[idx++] = endTime;
				} else if (CategoricalStatistics.COUNT.equals(type)) {
					out[idx++] = validCount;
				} else if (CategoricalStatistics.DISTINCT_COUNT.equals(type)) {
					HashMap <List <Object>, Integer> kvCount = kvCount(mt, fromId, endId, featureCols);
					out[idx++] = kvCount.size();
				} else if (CategoricalStatistics.TOTAL_COUNT.equals(type)) {
					out[idx++] = endId - fromId;
				} else if (CategoricalStatistics.RATIO.equals(type)) {
					if (type instanceof InterfaceLatestStatFeatures) {
						HashMap <List <Object>, Integer> kvCount = kvCount(mt, fromId, endId, featureCols);
						List <Object> objs = new ArrayList <>(colIndexs.length);
						for (int i : colIndexs) {
							objs.add(mt.getEntry(endId - 1, i));
						}
						out[idx++] = (double) kvCount.get(objs) / (double) validCount;
					} else {
						throw new AkUnsupportedOperationException("It is not support yet." + type);
					}
				} else if (type instanceof CategoricalStatistics.FirstN) {
					int n = ((CategoricalStatistics.FirstN) type).getN();
					if (fromId + n >= rowNum) {
						out[idx++] = null;
					} else {
						int[] featureIndex = TableUtil.findColIndicesWithAssert(mt.getColNames(), featureCols);
						StringBuilder sbd = new StringBuilder();
						for (int i = 0; i < featureIndex.length; i++) {
							sbd.append(mt.getEntry(fromId + n, featureIndex[i]));
							if(i!= featureIndex.length -1) {
								sbd.append("_");
							}
						}
						out[idx++] = sbd.toString();
					}
				} else if (type instanceof CategoricalStatistics.LastN) {
					int n = ((CategoricalStatistics.LastN) type).getN();
					if (endId - n < 0) {
						out[idx++] = null;
					} else {
						int[] featureIndex = TableUtil.findColIndicesWithAssert(mt.getColNames(), featureCols);
						StringBuilder sbd = new StringBuilder();
						for (int i = 0; i < featureIndex.length; i++) {
							sbd.append(mt.getEntry(endId - n, featureIndex[i]));
							if(i!= featureIndex.length -1) {
								sbd.append("_");
							}
						}
						out[idx++] = sbd.toString();
					}
				} else if (type instanceof KvStat) {
					int topN = ((KvStat) type).getN();
					topN = topN > 0 ? topN : endId - fromId;
					RankType rankType = ((KvStat) type).getRankType();
					int lastValue = 0;
					HashMap <List <Object>, Integer> counts = kvCount(mt, fromId, endId, featureCols);

					PriorityQueue <Tuple2 <List <Object>, Integer>> queue = new PriorityQueue <>(topN,
						new Comparator <Tuple2 <List <Object>, Integer>>() {
							@Override
							public int compare(Tuple2 <List <Object>, Integer> o1,
											   Tuple2 <List <Object>, Integer> o2) {
								return o2.f1 - o1.f1;
							}
						});
					for (Entry <List <Object>, Integer> entry : counts.entrySet()) {
						queue.add(Tuple2.of(entry.getKey(), entry.getValue()));
					}

					StringBuilder sbd = new StringBuilder();
					for (int rank = 0; rank <= topN; ) {
						if (queue.peek() == null) {
							break;
						}
						Tuple2 <List <Object>, Integer> rank_i = queue.poll();

						if (RankType.DENSE_RANK.equals(rankType) && rank_i.f1 < lastValue) {
							rank++;
							if (rank > topN) {break;}
						} else if (RankType.RANK.equals(rankType)) {
							rank++;
							if (rank > topN && rank_i.f1 == lastValue) {
								topN++;
							} else if (rank > topN) {
								break;
							}
						}

						lastValue = rank_i.f1;
						for (int j = 0; j < colIndexs.length; j++) {
							sbd.append(mt.getColNames()[colIndexs[j]])
								.append(":")
								.append(rank_i.f0.get(j))
								.append("_");
						}
						sbd.append(rank_i.f1).append("_");
					}
					out[idx++] = sbd.substring(0, sbd.length() - 1);
				} else if (type.equals(CategoricalStatistics.IS_EXIST)) {
					int[] featureIndex = TableUtil.findColIndicesWithAssert(mt.getColNames(), featureCols);
					Object[] obj = new Object[featureIndex.length];
					for (int i = 0; i < featureIndex.length; i++) {
						obj[i] = mt.getEntry(endId - 1, featureIndex[i]);
					}
					Boolean exist = false;
					for (int i = endId - 2; i >= fromId; i--) {
						int j = 0;
						while (j < featureIndex.length && mt.getEntry(i, featureIndex[j]).equals(obj[j])) {
							j++;
						}
						if (j == featureIndex.length) {
							exist = true;
							break;
						}
					}
					out[idx++] = exist;
				} else {
					throw new AkUnsupportedOperationException("It is not support yet." + type);
				}
			}
		}
	}

	public static TableSchema getOutMTableSchema(TableSchema inputSchema, List <BaseStatFeatures <?>> features) {
		String[] outColNames = inputSchema.getFieldNames();
		TypeInformation <?>[] outColTypes = inputSchema.getFieldTypes();
		for (BaseStatFeatures <?> feature : features) {
			outColNames = mergeColNames(outColNames, feature.getOutColNames());
			outColTypes = mergeColTypes(outColTypes, feature.getOutColTypes(inputSchema));
		}
		return new TableSchema(outColNames, outColTypes);
	}

	public static TableSchema getOutMTableSchema(TableSchema inputSchema, BaseStatFeatures <?> feature) {
		String[] outColNames = mergeColNames(inputSchema.getFieldNames(),
			feature.getOutColNames());
		TypeInformation <?>[] outColTypes = mergeColTypes(inputSchema.getFieldTypes(),
			feature.getOutColTypes(inputSchema));
		return new TableSchema(outColNames, outColTypes);
	}

	public static String[] mergeColNames(String[]... cols) {
		int length = 0;
		for (String[] col : cols) {
			length += col.length;
		}
		String[] outCols = new String[length];
		int startIdx = 0;
		for (String[] col : cols) {
			System.arraycopy(col, 0, outCols, startIdx, col.length);
			startIdx += col.length;
		}
		return outCols;
	}

	public static TypeInformation <?>[] mergeColTypes(TypeInformation <?>[]... colTypes) {
		int length = 0;
		for (TypeInformation <?>[] colType : colTypes) {
			length += colType.length;
		}
		TypeInformation <?>[] outCols = new TypeInformation <?>[length];
		int startIdx = 0;
		for (TypeInformation <?>[] colType : colTypes) {
			System.arraycopy(colType, 0, outCols, startIdx, colType.length);
			startIdx += colType.length;
		}
		return outCols;
	}

	public static BatchOperator <?> group2MTables(BatchOperator <?> inputData, String[] groupCols) {
		String groupByPredicate = groupCols.length == 0 ? "1" : Joiner.on(", ").join(groupCols);
		String selectClause = String.format("MTABLE_AGG(%s) as %s",
			String.join(", ", inputData.getColNames()),
			TEMP_MTABLE_COL
		);

		return inputData.groupBy(groupByPredicate, selectClause);
	}

	//merge features by group cols.
	public static Map <String[], Tuple2 <List <BaseStatFeatures <?>>, List <Integer>>> mergeFeatures(
		BaseStatFeatures <?>[] features) {
		Map <String[], Tuple2 <List <BaseStatFeatures <?>>, List <Integer>>> mergedFeatures =
			new TreeMap <String[], Tuple2 <List <BaseStatFeatures <?>>, List <Integer>>>(
				new Comparator <String[]>() {
					@Override
					public int compare(String[] o1, String[] o2) {
						String deli = "|";
						String str1 = String.join(deli, o1);
						String str2 = String.join(deli, o2);
						return str1.compareTo(str2);
					}
				}
			);

		BaseStatFeatures <?>[] flattenedFeatures = flattenFeatures(features);
		int idx = 0;
		for (BaseStatFeatures <?> feature : flattenedFeatures) {
			String[] groupCols = feature.groupCols;
			Tuple2 <List <BaseStatFeatures <?>>, List <Integer>> featuresList = mergedFeatures.getOrDefault(groupCols,
				Tuple2.of(new ArrayList <BaseStatFeatures <?>>(), new ArrayList <Integer>()));
			featuresList.f0.add(feature);
			featuresList.f1.add(idx);
			mergedFeatures.put(groupCols, featuresList);
			idx++;
		}
		return mergedFeatures;
	}

	public static Map <String[], List <BaseDaysStatFeatures <?>>> mergeFeatures(
		BaseDaysStatFeatures <?>[] features) {
		Map <String[], List <BaseDaysStatFeatures <?>>> mergedFeatures =
			new TreeMap <String[], List <BaseDaysStatFeatures <?>>>(
				new Comparator <String[]>() {
					@Override
					public int compare(String[] o1, String[] o2) {
						String deli = "|";
						String str1 = String.join(deli, o1);
						String str2 = String.join(deli, o2);
						return str1.compareTo(str2);
					}
				}
			);

		for (BaseDaysStatFeatures <?> feature : features) {
			String[] groupCols = feature.groupCols == null ? new String[] {} : feature.groupCols;
			List <BaseDaysStatFeatures <?>> featuresList = mergedFeatures.getOrDefault(groupCols,
				new ArrayList <BaseDaysStatFeatures <?>>());
			featuresList.add(feature);
			mergedFeatures.put(groupCols, featuresList);
		}

		Map <String[], List <BaseDaysStatFeatures <?>>> mergedFeatures2 =
			new TreeMap <String[], List <BaseDaysStatFeatures <?>>>(
				new Comparator <String[]>() {
					@Override
					public int compare(String[] o1, String[] o2) {
						String deli = "|";
						String str1 = String.join(deli, o1);
						String str2 = String.join(deli, o2);
						return str1.compareTo(str2);
					}
				}
			);

		//deal with count.
		for (Map.Entry <String[], List <BaseDaysStatFeatures <?>>> entry : mergedFeatures.entrySet()) {
			String[] groupCols = entry.getKey();
			if (null == groupCols || groupCols.length == 0) {
				groupCols = new String[] {GenerateFeatureOfLatestNDaysBatchOp.DEFAULT_GROUP_COL};
			}
			List <BaseDaysStatFeatures <?>> features1Group = new ArrayList <>();
			for (BaseDaysStatFeatures <?> feature : entry.getValue()) {
				if (feature instanceof NDaysNumericStatFeatures) {
					NDaysNumericStatFeatures nFeature = (NDaysNumericStatFeatures) feature;
					boolean hasTotalCount = false;
					BaseNumericStatistics[] stats = nFeature.getNumericStatistics();
					List <BaseNumericStatistics> statList = new ArrayList <>();
					for (BaseNumericStatistics stat : stats) {
						if (NumericDaysStatistics.TOTAL_COUNT == stat) {
							hasTotalCount = true;
						} else {
							statList.add(stat);
						}
					}

					if (hasTotalCount) {
						features1Group.add(
							new NDaysNumericStatFeatures()
								.setGroupCols(groupCols)
								.setNumericDaysStatistics(NumericDaysStatistics.TOTAL_COUNT)
								.setFeatureCols(nFeature.getFeatureCols()[0])
								.setNDays(feature.nDays)
								.setConditions(nFeature.conditions)
								.setConditionCol(nFeature.conditionCol)
						);
					}

					features1Group.add(
						new NDaysNumericStatFeatures()
							.setGroupCols(groupCols)
							.setNumericDaysStatistics(statList.toArray(new BaseNumericStatistics[0]))
							.setFeatureCols(nFeature.featureCols)
							.setNDays(feature.nDays)
							.setConditions(nFeature.conditions)
							.setConditionCol(nFeature.conditionCol)
					);

				} else if (feature instanceof NDaysCategoricalStatFeatures) {
					NDaysCategoricalStatFeatures cFeature = (NDaysCategoricalStatFeatures) feature;
					boolean hasTotalCount = false;
					BaseCategoricalStatistics[] stats = cFeature.getCategoricalStatistics();
					List <BaseCategoricalStatistics> statList = new ArrayList <>();
					for (BaseCategoricalStatistics stat : stats) {
						if (CategoricalStatistics.COUNT == stat) {
							hasTotalCount = true;
						} else {
							statList.add(stat);
						}
					}

					if (hasTotalCount) {
						features1Group.add(
							new NDaysCategoricalStatFeatures()
								.setGroupCols(groupCols)
								.setCategoricalDaysStatistics(CategoricalDaysStatistics.TOTAL_COUNT)
								.setFeatureCols(cFeature.getFeatureCols()[0])
								.setNDays(cFeature.nDays)
								.setConditions(cFeature.conditions)
								.setConditionCol(cFeature.conditionCol)
						);
					}

					features1Group.add(
						new NDaysCategoricalStatFeatures()
							.setGroupCols(groupCols)
							.setCategoricalDaysStatistics(statList.toArray(new BaseCategoricalStatistics[0]))
							.setFeatureCols(cFeature.featureCols)
							.setNDays(cFeature.nDays)
							.setFeatureItems(cFeature.featureItems)
							.setConditions(cFeature.conditions)
							.setConditionCol(cFeature.conditionCol)
					);
				}
			}
			mergedFeatures2.put(groupCols, features1Group);
		}

		return mergedFeatures2;
	}

	public static BaseStatFeatures <?>[] flattenFeatures(BaseStatFeatures <?>[] features) {
		List <BaseStatFeatures <?>> flattenedFeatures = new ArrayList <BaseStatFeatures <?>>();
		for (BaseStatFeatures <?> feature : features) {
			flattenedFeatures.addAll(Arrays.asList(feature.flattenFeatures()));
		}
		return flattenedFeatures.toArray(new BaseStatFeatures <?>[0]);
	}

	public static String groupColsToKv(MTable mt, String[] groupCols) {
		return groupColsToKv(mt.getRow(0), groupCols, mt.getColNames());

	}

	public static String groupColsToKv(Row row, String[] groupCols, String[] colNames) {
		int[] groupIndices = TableUtil.findColIndices(colNames, groupCols);
		String[] groupVals = new String[groupIndices.length];
		for (int i = 0; i < groupVals.length; i++) {
			groupVals[i] = groupCols[i] + ":" + row.getField(groupIndices[i]);
		}
		return String.join(" ", groupVals);

	}

	static Tuple2 <String, Integer> getTimeUnit(String windowTime) {
		return Tuple2.of(windowTime.substring(windowTime.length() - 1),
			Integer.parseInt(windowTime.substring(0, windowTime.length() - 1)));
	}

	static Timestamp getNextTime(Timestamp time, String unit, int ti) {
		LocalDateTime localTime = time.toLocalDateTime();

		switch (unit) {
			case "s":
			case "m":
			case "h":
			case "d":
				return new Timestamp(time.getTime() + getIntervalByMS(unit, ti));
			case "M":
				return Timestamp.valueOf(localTime
					.plusMonths(ti));
			case "y":
				return Timestamp.valueOf(localTime
					.plusYears(ti));
			default:
				throw new AkUnsupportedOperationException("It is not support yet.");
		}
	}

	public static long getIntervalByMS(String windowTime) {
		int ti = getTimeUnit(windowTime).f1;
		String unit = getTimeUnit(windowTime).f0;
		return getIntervalByMS(unit, ti);
	}

	public static long getIntervalBySecond(String windowTime) {
		return getIntervalByMS(windowTime) / 1000;
	}

	static long getIntervalByMS(String unit, int ti) {
		switch (unit) {
			case "s":
				return ti * 1000L;
			case "m":
				return ti * 60_000L;
			case "h":
				return ti * 3600_000L;
			case "d":
				return ti * 43200_000L;
			default:
				throw new AkUnsupportedOperationException("It is not support yet.");
		}
	}

	static Timestamp getStartTime(Timestamp firstTime, String unit, int ti) {
		LocalDateTime localFirstTime = firstTime.toLocalDateTime();

		switch (unit) {
			case "s":
			case "m":
			case "h":
			case "d":
				return trimTime(firstTime, getIntervalByMS(unit, ti));
			case "M":
				return Timestamp.valueOf(localFirstTime
					.withMonth(ti - 1)
					.withDayOfMonth(1)
					.withMinute(0)
					.withSecond(0)
					.withNano(0));
			case "y":
				return Timestamp.valueOf(localFirstTime
					.minusYears(ti - 1)
					.withMonth(1)
					.withDayOfMonth(1)
					.withMinute(0)
					.withSecond(0)
					.withNano(0));
			default:
				throw new AkUnsupportedOperationException("It is not support yet.");
		}
	}

	public static String[] getFeatureNames(BaseStatFeatures <?> feature, String timeCol) {
		String[] features = null;
		if (feature instanceof BaseNumericStatFeatures) {
			features = ((BaseNumericStatFeatures <?>) feature).featureCols;
		} else if (feature instanceof BaseCategoricalStatFeatures) {
			features = ((BaseCategoricalStatFeatures <?>) feature).featureCols;
		} else if (feature instanceof BaseCrossCategoricalStatFeatures) {
			String[][] crossFeatures = ((BaseCrossCategoricalStatFeatures <?>) feature).crossFeatureCols;
			features = mergeCols(crossFeatures);
		}
		return mergeCols(features, timeCol);

	}

	private static Timestamp trimTime(Timestamp time, long interval) {
		return new Timestamp(time.getTime() / interval * interval);
	}

	private static String[] mergeCols(String[][] cols) {
		Set <String> colSet = new HashSet <String>();
		for (String[] col : cols) {
			Collections.addAll(colSet, col);
		}
		return colSet.toArray(new String[0]);
	}

	private static String[] mergeCols(String[] cols, String timeCol) {
		Set <String> colSet = new HashSet <String>();
		Collections.addAll(colSet, cols);
		colSet.add(timeCol);
		return colSet.toArray(new String[0]);
	}

	public static TableSchema getWindowOutSchema(BaseStatFeatures <?> feature, TableSchema inSchema) {
		String[] groupCols = feature.groupCols;
		String startTimeCol = String.join("_", feature.getColPrefix(), "start_time");
		String endTimeCol = String.join("_", feature.getColPrefix(), "end_time");

		String[] featureOutCols = GenerateFeatureUtil.mergeColNames(groupCols,
			new String[] {startTimeCol, endTimeCol}, feature.getOutColNames());
		TypeInformation <?>[] featureOutTypes = GenerateFeatureUtil.mergeColTypes(
			TableUtil.findColTypes(inSchema, groupCols),
			new TypeInformation <?>[] {Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP},
			feature.getOutColTypes(inSchema));

		return new TableSchema(featureOutCols, featureOutTypes);
	}

	public static Row setOutRow(int outColNum, Row value, int[] groupColIndices, int groupColNum,
								Timestamp startTime, Timestamp endTime, int outFeatureNum, Object[] outObj) {
		Row outRow = new Row(outColNum);
		for (int i = 0; i < groupColNum; i++) {
			outRow.setField(i, value.getField(groupColIndices[i]));
		}

		outRow.setField(groupColNum, startTime);
		outRow.setField(groupColNum + 1, endTime);

		for (int i = 0; i < outFeatureNum; i++) {
			outRow.setField(groupColNum + 2 + i, outObj[i]);
		}

		return outRow;
	}

}
