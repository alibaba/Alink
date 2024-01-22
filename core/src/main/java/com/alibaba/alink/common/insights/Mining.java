package com.alibaba.alink.common.insights;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.probabilistic.CDF;
import com.alibaba.alink.operator.common.regression.LinearReg;
import com.alibaba.alink.operator.common.regression.LinearRegressionModel;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.operator.common.statistics.statistics.SrtUtil;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;
import com.alibaba.alink.operator.common.statistics.statistics.WindowTable;
import com.alibaba.alink.operator.common.timeseries.TsMethod;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.dataproc.AppendIdLocalOp;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.params.statistics.HasStatLevel_L1.StatLevel;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Mining {

	public static final String MEASURE_NAME_PREFIX = "measure_";

	public static LocalOperator <?> filter(LocalOperator <?> source, List <Subspace> subspaces) {
		if (subspaces.size() > 0) {
			StringBuilder sbd = new StringBuilder();
			sbd.append(createFilterClause(subspaces.get(0)));
			for (int i = 1; i < subspaces.size(); i++) {
				sbd.append(" AND ").append(createFilterClause(subspaces.get(i)));
			}
			//System.out.println("filter sql: " + sbd.toString());
			return source.filter(sbd.toString());
		} else {
			return source;
		}
	}

	public static Insight calcInsight(LocalOperator <?> source, Subject subject, InsightType type) {
		source = filter(source, subject.subspaces);

		StringBuilder sbdAggr = new StringBuilder();
		sbdAggr.append(subject.breakdown.colName);
		for (int i = 0; i < subject.measures.size(); i++) {
			Measure measure = subject.measures.get(i);
			sbdAggr.append(", ").append(measure.aggr).append("(").append(measure.colName).append(") AS ").append(
				MEASURE_NAME_PREFIX).append(i);
		}

		LocalOperator <?> dataAggr = source.groupBy(subject.breakdown.colName, sbdAggr.toString());

		//dataAggr.lazyPrint(100, "------ agg -------- ");
		//System.out.println();

		return calcInsight(subject, dataAggr, type);

	}

	public static Insight calcInsight(Subject subject, LocalOperator <?> dataAggr, InsightType type) {
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = type;

		switch (type) {
			case OutstandingNo1:
				insight = outstandingNo1(dataAggr, subject);
				break;
			case OutstandingLast:
				insight = outstandingNoLast(dataAggr, subject);
				break;
			case OutstandingTop2:
				insight = outstandingTop2(dataAggr, subject);
				break;
			case Evenness:
				insight = even(dataAggr, subject);
				break;
			case Attribution:
				insight = attribution(dataAggr, subject);
				break;
			case ChangePoint:
				insight = changePoint(dataAggr, subject);
				break;
			case Outlier:
				insight = outlier(dataAggr, subject);
				break;
			case Trend:
				insight = trend(dataAggr, subject);
				break;
			case Seasonality:
				insight = seasonality(dataAggr, subject);
				break;
			default:
				throw new AkIllegalOperatorParameterException("Insight type not support yet!" + type.name());
		}

		return insight;
	}

	private static Insight outstandingNo1(LocalOperator <?> dataAggr, Subject subject) {
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.OutstandingNo1;
		insight.score = 0;

		String measureCol = MEASURE_NAME_PREFIX + "0";

		TableSummary summary = dataAggr.collectStatistics();

		// 一个值，没有必要； 两个值，直接比较；三个值，去掉最大值，只剩两个，那么残差必然是0，所以至少要有4个点
		long rowNum = summary.count();
		double max = summary.maxDouble(measureCol);
		double sum = summary.sum(measureCol);
		if (subject.measures.size() != 1 ||
			rowNum <= 2 ||
			summary.min(measureCol) == summary.max(measureCol) ||
			max < 0 ||
			max / sum < 0.1) {
			return insight;
		}

		List <Double> values = loadData(dataAggr.collect(), 1);
		// get maxValueKey
		int iMaxIdx = 0;
		double minObj = values.get(0);
		for (int iMax = 1; iMax < values.size(); iMax++) {
			if (values.get(iMax) > minObj) {
				minObj = values.get(iMax);
				iMaxIdx = iMax;
			}
		}
		String maxValueKey = objToString(dataAggr.getOutputTable().getEntry(iMaxIdx, 0));

		double score = 0;
		if (rowNum == 3) {
			score = max / sum;
		} else {
			score = outstandingNo1PValue(values.toArray(new Double[0]), 0.7, max);
		}
		insight.score = score;

		Measure measure = insight.subject.measures.get(0);
		Subspace subspace = subject.subspaces.isEmpty() ? null : subject.subspaces.get(0);

		LayoutData layoutData = new LayoutData();
		layoutData.data = dataAggr.getOutputTable();
		layoutData.title = insight.getTitle();
		layoutData.description = (subspace == null ? "" : subspace.strInDescription()) +
			String.format("%s里%s的值明显高于其余的值.", subject.breakdown.colName, maxValueKey);
		layoutData.focus = String.format("%s", maxValueKey);
		layoutData.xAxis = String.format("%s", subject.breakdown.colName);
		layoutData.yAxis = String.format("%s(%s)", measure.aggr, measure.colName);

		insight.layout = layoutData;

		return insight;
	}

	private static Insight outstandingNoLast(LocalOperator <?> dataAggr, Subject subject) {
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.OutstandingLast;
		insight.score = 0;

		String measureCol = MEASURE_NAME_PREFIX + "0";

		TableSummary summary = dataAggr.collectStatistics();

		long rowNum = summary.count();
		double max = summary.maxDouble(measureCol);
		double min = summary.minDouble(measureCol);

		if (subject.measures.size() != 1 ||
			rowNum <= 0 ||
			max > 0 ||
			summary.min(measureCol) == summary.max(measureCol)) {
			return insight;
		}

		List <Double> values = loadData(dataAggr.collect(), 1);
		values.replaceAll(aDouble -> -aDouble);

		double score = 0;
		if (rowNum == 2) {
			score = Math.abs(min) / (Math.abs(values.get(0)) + Math.abs(values.get(1)));
		} else if (rowNum == 3) {
			score = Math.abs(min) /
				(Math.abs(values.get(0)) + Math.abs(values.get(1)) + Math.abs(values.get(2)));
		} else {
			score = outstandingNo1PValue(values.toArray(new Double[0]), 0.7, Math.abs(min));
		}

		insight.score = score;

		// get minValueKey
		int iMinIdx = 0;
		double minObj = values.get(0);
		for (int iMin = 1; iMin < rowNum; iMin++) {
			if (values.get(iMin) > minObj) {
				minObj = values.get(iMin);
				iMinIdx = iMin;
			}
		}
		String minValueKey = objToString(dataAggr.getOutputTable().getEntry(iMinIdx, 0));

		Measure measure = insight.subject.measures.get(0);
		Subspace subspace = subject.subspaces.isEmpty() ? null : subject.subspaces.get(0);

		LayoutData layoutData = new LayoutData();
		layoutData.data = dataAggr.getOutputTable();
		layoutData.title = insight.getTitle();
		layoutData.description = (subspace == null ? "" : subspace.strInDescription()) +
			String.format("%s里最大负值%s明显低于其余的值.", subject.breakdown.colName, minValueKey);
		layoutData.focus = String.format("%s", minValueKey);
		layoutData.xAxis = String.format("%s", subject.breakdown.colName);
		layoutData.yAxis = String.format("%s(%s)", measure.aggr, measure.colName);

		insight.layout = layoutData;

		return insight;
	}

	private static Insight outstandingTop2(LocalOperator <?> dataAggr, Subject subject) {
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.OutstandingTop2;
		insight.score = 0;

		String measureCol = MEASURE_NAME_PREFIX + "0";
		TableSummary summary = dataAggr.collectStatistics();
		long rowNum = summary.count();

		if (subject.measures.size() != 1 ||
			summary.min(measureCol) == summary.max(measureCol) ||
			summary.minDouble(measureCol) < 0 ||
			rowNum <= 2) {
			return insight;
		}

		double max = Double.NEGATIVE_INFINITY;
		double max2 = Double.NEGATIVE_INFINITY;
		double sumT = 0;

		List <Double> values = loadData(dataAggr.collect(), 1);

		Double[] vals = values.toArray(new Double[0]);
		int firstMaxIdx = 0;
		int secondMaxIdx = 0;
		int idx = 0;
		for (Double datum : vals) {
			if (datum < max2) {
			} else if (datum > max2 && datum < max) {
				max2 = datum;
				secondMaxIdx = idx;
			} else {
				max2 = max;
				max = datum;
				secondMaxIdx = firstMaxIdx;
				firstMaxIdx = idx;
			}
			sumT += datum;
			idx++;
		}

		String firstMaxValue = objToString(dataAggr.getOutputTable().getEntry(firstMaxIdx, 0));
		String secondMaxValue = objToString(dataAggr.getOutputTable().getEntry(secondMaxIdx, 0));

		if (max / sumT >= 0.5 ||
			//(max + max2) / summary.sum(measureCol) < 0.5 ||
			(max + max2) / summary.sum(measureCol) < 0.2) {
			return insight;
		}

		double score = 0;
		if (rowNum <= 4) {
			score = (max + max2) / summary.sum(measureCol);
		} else {
			score = outstandingTop2PValue(vals, 0.7);
		}

		// construct description.
		insight.score = score;

		Measure measure = insight.subject.measures.get(0);
		Subspace subspace = subject.subspaces.isEmpty() ? null : subject.subspaces.get(0);

		LayoutData layoutData = new LayoutData();
		layoutData.data = dataAggr.getOutputTable();
		layoutData.title = insight.getTitle();
		layoutData.description = (subspace == null ? "" : subspace.strInDescription()) +
			String.format("%s里%s和%s的值明显高于其余的值.", subject.breakdown.colName, firstMaxValue, secondMaxValue);
		layoutData.focus = String.format("%s %s", firstMaxValue, secondMaxValue);
		layoutData.xAxis = String.format("%s", subject.breakdown.colName);
		layoutData.yAxis = String.format("%s(%s)", measure.aggr, measure.colName);

		insight.layout = layoutData;

		return insight;
	}

	private static Insight even(LocalOperator <?> dataAggr, Subject subject) {
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.Evenness;
		insight.score = 0;

		String measureCol = MEASURE_NAME_PREFIX + "0";

		TableSummary summary = dataAggr.collectStatistics();

		if (subject.measures.size() != 1 ||
			summary.count() < 5 ||
			summary.count() > 20) {
			return insight;
		}

		double mean = summary.mean(measureCol);

		if (summary.min(measureCol) == summary.max(measureCol)) {
			insight.score = 0.8;
		} else {
			List <Double> values = loadData(dataAggr.collect(), 1);

			// construct description.
			insight.score = chiSquare(values, mean);
		}

		Measure measure = insight.subject.measures.get(0);
		Subspace subspace = subject.subspaces.isEmpty() ? null : subject.subspaces.get(0);

		LayoutData layoutData = new LayoutData();
		layoutData.data = dataAggr.getOutputTable();
		layoutData.title = insight.getTitle();
		layoutData.description = (subspace == null ? "" : subspace.strInDescription()) +
			String.format("%s所有的值非常接近.", subject.breakdown.colName);
		layoutData.focus = null;
		layoutData.xAxis = String.format("%s", subject.breakdown.colName);
		layoutData.yAxis = String.format("%s(%s)", measure.aggr, measure.colName);
		layoutData.lineA = String.format("[%s, %s]", 0, mean);

		insight.layout = layoutData;

		return insight;
	}

	private static Insight attribution(LocalOperator <?> dataAggr, Subject subject) {
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.Attribution;
		insight.score = 0;

		String measureCol = MEASURE_NAME_PREFIX + "0";

		TableSummary summary = dataAggr.collectStatistics();
		double max = summary.maxDouble(measureCol);
		double sum = summary.sum(measureCol);

		if (subject.measures.size() != 1 ||
			summary.minDouble(measureCol) < 0 ||
			max / sum < 0.5) {
			return insight;
		}

		// get max value
		List <Double> values = loadData(dataAggr.collect(), 1);

		int iMaxIdx = 0;
		double minObj = values.get(0);
		for (int iMax = 1; iMax < values.size(); iMax++) {
			if (values.get(iMax) > minObj) {
				minObj = values.get(iMax);
				iMaxIdx = iMax;
			}
		}
		String maxValueKey = objToString(dataAggr.getOutputTable().getEntry(iMaxIdx, 0));

		Insight outstandingNo1Insight = outstandingNo1(dataAggr, subject);
		insight.score = Math.min(outstandingNo1Insight.score * 1.001, 1.0);

		Measure measure = insight.subject.measures.get(0);
		Subspace subspace = subject.subspaces.isEmpty() ? null : subject.subspaces.get(0);

		LayoutData layoutData = new LayoutData();
		layoutData.data = dataAggr.getOutputTable();
		layoutData.title = insight.getTitle();
		layoutData.description = (subspace == null ? "" : subspace.strInDescription()) +
			String.format("%s里%s占比超过一半.", subject.breakdown.colName, maxValueKey);

		layoutData.focus = maxValueKey;
		layoutData.xAxis = String.format("%s", subject.breakdown.colName);
		layoutData.yAxis = String.format("%s(%s)", measure.aggr, measure.colName);

		insight.layout = layoutData;

		return insight;
	}

	/**
	 * 变点的前k个值和后k个值，是两个不同的分布。使用独立性T检验判断，找出所有的change point。再计算最大的score。
	 */
	private static Insight changePoint(LocalOperator <?> dataAggr, Subject subject) {
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.ChangePoint;
		insight.score = 0;

		if (subject.measures.size() != 1) {
			return insight;
		}

		MTable mt = dataAggr.getOutputTable();
		mt.orderBy(subject.breakdown.colName);

		List <Double> values = loadData(mt.getRows(), 1);

		Double[] vals = values.toArray(new Double[0]);
		Integer[] changePointIndices = null;
		if (vals.length >= 20) {
			changePointIndices = findAllChangePointId(vals);
		} else {
			List <Integer> changePointList = new ArrayList <>();
			double[] signs = new double[vals.length];
			signs[0] = 0;
			for (int i = 1; i < vals.length; i++) {
				signs[i] = vals[i] - vals[i - 1];
			}
			for (int i = 2; i < vals.length - 1; i++) {
				if (signs[i] * signs[i - 1] < 0) {
					changePointList.add(i - 1);
				}
			}
			changePointIndices = changePointList.toArray(new Integer[0]);
		}

		String[] changePointTimeString = new String[changePointIndices.length];
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < changePointTimeString.length; i++) {
			if (mt.getEntry(changePointIndices[i], 0) != null) {
				changePointTimeString[i] = sdf.format(mt.getEntry(changePointIndices[i], 0));
				//changePointTimeString[i] = sdf.format(
				//	Timestamp.valueOf(LocalDateTime.ofInstant(((Timestamp) mt.getEntry(changePointIndices[i],
				//		0)).toInstant(), ZoneOffset.UTC)));
				sbd.append(",").append(changePointTimeString[i]);
			}
		}

		double scoreMax = 0;
		int k = 20;
		for (Integer changePointIndex : changePointIndices) {
			int startIdx = Math.max(0, changePointIndex - k);
			int endIdx = Math.min(vals.length, changePointIndex + k);
			double score = changePointScore(vals, startIdx, endIdx, changePointIndex);
			if (score > scoreMax) {
				scoreMax = score;
			}
		}
		insight.score = changePointIndices.length == 0 ? 0.01 : scoreMax;

		Measure measure = insight.subject.measures.get(0);
		Subspace subspace = subject.subspaces.isEmpty() ? null : subject.subspaces.get(0);

		LayoutData layoutData = new LayoutData();
		layoutData.data = dataAggr.getOutputTable();
		layoutData.title = (subspace == null ? "" : subspace.strInDescription()) +
			String.format("在%s下，时间序列%s(%s)的变点",
				subject.breakdown.colName, measure.colName, measure.aggr.getCnName());

		if (changePointIndices.length == 0) {
			layoutData.description = "时间序列没有变点.";
		} else {
			layoutData.description = String.format("时间序列有%s个变点，分别是: %s.",
				changePointIndices.length, sbd.substring(1));
			layoutData.focus = sbd.substring(1);
		}
		layoutData.xAxis = String.format("%s", subject.breakdown.colName);
		layoutData.yAxis = String.format("%s(%s)", measure.aggr, measure.colName);

		insight.layout = layoutData;

		return insight;
	}

	/**
	 * KSigma Outlier.
	 */
	private static Insight outlier(LocalOperator <?> dataAggr, Subject subject) {
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.Outlier;
		insight.score = 0;

		if (subject.measures.size() != 1) {
			return insight;
		}

		String measureCol = MEASURE_NAME_PREFIX + "0";

		MTable mt = dataAggr.getOutputTable();
		mt.orderBy(subject.breakdown.colName);

		List <Double> valueList = loadData(mt.getRows(), 1);
		double[] values = new double[valueList.size()];
		int idx = 0;
		for (Double value : valueList) {
			values[idx] = value;
			idx++;
		}

		double[] zScores = values.clone();

		TableSummary summary = dataAggr.select(measureCol).getOutputTable().summary();
		final double mean = summary.mean(measureCol);
		final double standardDeviation = summary.standardDeviation(measureCol);

		List <String> outlierTimeString = new ArrayList <>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		StringBuilder sbd = new StringBuilder();

		for (int i = 0; i < zScores.length; i++) {
			if (standardDeviation != 0.0) {
				zScores[i] = Math.abs(zScores[i] - mean) / standardDeviation;
			} else {
				zScores[i] = 0.0;
			}

			if (zScores[i] > 3) {
				if (mt.getEntry(i, 0) != null) {
					outlierTimeString.add(sdf.format(mt.getEntry(i, 0)));
					sbd.append(",").append(sdf.format(mt.getEntry(i, 0)));
				}
			}
		}

		double max = Double.NEGATIVE_INFINITY;
		for (double score : zScores) {
			max = Math.max(max, score);
		}

		// construct insight.
		insight.score = outlierTimeString.size() == 0 ? 0.01 : 1 - 2 * CDF.stdNormal(-max);

		LayoutData layoutData = new LayoutData();
		layoutData.data = mt;

		Measure measure = insight.subject.measures.get(0);
		Subspace subspace = subject.subspaces.isEmpty() ? null : subject.subspaces.get(0);

		layoutData.title = (subspace == null ? "" : subspace.strInDescription()) +
			String.format("在%s下，时间序列%s(%s)的异常点",
				subject.breakdown.colName, measure.colName, measure.aggr.getCnName());
		if (outlierTimeString.size() == 0) {
			layoutData.description = "时间序列没有异常点";
		} else {
			layoutData.description = String.format("时间序列有%s个异常点，分别是: %s.",
				outlierTimeString.size(), sbd.substring(1));
			layoutData.focus = sbd.substring(1);
		}

		layoutData.xAxis = String.format("%s", subject.breakdown.colName);
		layoutData.yAxis = String.format("%s(%s)", measure.aggr, measure.colName);

		insight.layout = layoutData;

		return insight;
	}

	/**
	 * H0: 没有趋势; H1: 有升或者降趋势.
	 * ref: Extrating Top-K Insights from Multi-dimensional Data.
	 * <a href="https://zhuanlan.zhihu.com/p/112703276?utm_id=0">...</a>
	 */
	private static Insight trend(LocalOperator <?> dataAggr, Subject subject) {
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.Trend;
		insight.score = 0;

		MTable mt = dataAggr.getOutputTable();
		mt.orderBy(subject.breakdown.colName);

		String measureCol = MEASURE_NAME_PREFIX + "0";

		LocalOperator <?> data = new MemSourceLocalOp(mt)
			.link(new AppendIdLocalOp().setIdCol("__alink_id__"))
			.select("__alink_id__, " + measureCol);

		String[] colNames = new String[] {"x", "y"};
		Class[] colTypes = new Class[] {Double.class, Double.class};

		WindowTable wt = new WindowTable(colNames, colTypes, data.getOutputTable().getRows());
		SummaryResultTable srt = SrtUtil.batchSummary(wt, colNames,
			1, 1, 1, 1, StatLevel.L3);

		LinearRegressionModel lrModel = LinearReg.train(srt, colNames[0], new String[] {colNames[1]});
		double r2 = lrModel.R2;
		double slope = lrModel.beta[1];

		double p = 1 - 1 / (1 + Math.exp(-(slope - 0.2) / 2));

		// construct insight.
		insight.score = r2 * (1 - p);

		LayoutData layoutData = new LayoutData();
		layoutData.data = mt;

		Measure measure = insight.subject.measures.get(0);
		Subspace subspace = subject.subspaces.isEmpty() ? null : subject.subspaces.get(0);

		layoutData.title = (subspace == null ? "" : subspace.strInDescription()) +
			String.format("在%s下，时间序列%s(%s)的趋势",
				subject.breakdown.colName, measure.colName, measure.aggr.getCnName());
		layoutData.description = String.format("时间序列有%s趋势.",
			slope > 0 ? "上升" : "下降");
		layoutData.focus = null;
		layoutData.xAxis = String.format("%s", subject.breakdown.colName);
		layoutData.yAxis = String.format("%s(%s)", measure.aggr, measure.colName);
		layoutData.lineA = String.format("[%s, %s]", slope, lrModel.beta[0]);

		insight.layout = layoutData;

		return insight;

	}

	/**
	 * pattern presents the repeated pattern in a time series.
	 * 按照多个序列计算相关系数的方式: ACF
	 * ref: <a href="https://www.microsoft.com/en-us/research/uploads/prod/2021/03/metainsight-extended.pdf">...</a>
	 * <a href="https://www.microsoft.com/en-us/research/uploads/prod/2016/12/Insight-Types-Specification<a href=".pdf">...</a>
	 * ">* https://zhuanlan.zhihu.com/p/9318</a>6317?utm_source=qq
	 */

	private static Insight seasonality(LocalOperator <?> dataAggr, Subject subject) {
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.Seasonality;
		insight.score = 0;

		if (subject.measures.size() != 1) {
			return insight;
		}

		MTable mt = dataAggr.getOutputTable();
		mt.orderBy(subject.breakdown.colName);

		List <Double> valueList = loadData(mt.getRows(), 1);
		double[] values = new double[valueList.size()];
		int idx = 0;
		for (Double value : valueList) {
			values[idx] = value;
			idx++;
		}

		ArrayList <double[]> acfAndConfidence = TsMethod.acf(values, Math.min(values.length / 2, 12));
		double[] acf = acfAndConfidence.get(0);
		double acfMax = Double.NEGATIVE_INFINITY;
		int acfMaxIdx = -1;
		for (int i = 1; i < acf.length; i++) {
			if (acfMax < acf[i]) {
				acfMax = acf[i];
				acfMaxIdx = i;
			}
		}

		if (acfMax <= 0 || acfMaxIdx < 2) {
			return insight;
		}

		//construct insight.
		insight.score = acfMax;

		LayoutData layoutData = new LayoutData();
		layoutData.data = mt;

		Measure measure = insight.subject.measures.get(0);
		Subspace subspace = subject.subspaces.isEmpty() ? null : subject.subspaces.get(0);

		layoutData.title = (subspace == null ? "" : subspace.strInDescription()) +
			String.format("在%s下，时间序列%s(%s)的季节性",
				subject.breakdown.colName, measure.colName, measure.aggr.getCnName());

		Timestamp t1 = (Timestamp) mt.getEntry(0, 0);
		Timestamp t2 = (Timestamp) mt.getEntry(acfMaxIdx, 0);

		layoutData.description = String.format("时间序列有季节性趋势, 周期是%s.", getPeriodStr(t1, t2));
		layoutData.focus = null;
		layoutData.xAxis = String.format("%s", subject.breakdown.colName);
		layoutData.yAxis = String.format("%s(%s)", measure.aggr, measure.colName);

		insight.layout = layoutData;

		return insight;
	}

	/**
	 * t2 - t1
	 */

	public static String getPeriodStr(Timestamp t1, Timestamp t2) {
		Period period = Period.between(t1.toLocalDateTime().toLocalDate(), t2.toLocalDateTime().toLocalDate());

		int years = period.getYears();
		int months = period.getMonths();
		int days = period.getDays();

		int SECONDS_PER_MINUTE = 60;
		int MINUTES_PER_HOUR = 60;
		int HOURS_PER_DAY = 24;
		int SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR;
		int SECONDS_PER_DAY = SECONDS_PER_HOUR * HOURS_PER_DAY;
		long timestampDiff = (t2.getTime() - t1.getTime()) / 1000 % SECONDS_PER_DAY;

		long hours = timestampDiff / SECONDS_PER_HOUR;
		int minutes = (int) ((timestampDiff % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
		int seconds = (int) (timestampDiff % SECONDS_PER_MINUTE);

		StringBuilder buf = new StringBuilder();
		if (years != 0) {
			buf.append(years).append("年");
		}
		if (months != 0) {
			buf.append(months).append("月");
		}
		if (days != 0) {
			buf.append(days).append("天");
		}

		if (hours != 0) {
			buf.append(hours).append("小时");
		}
		if (minutes != 0) {
			buf.append(minutes).append("分钟");
		}
		if (seconds != 0) {
			buf.append(seconds).append("秒");
		}
		return buf.toString();
	}

	/**
	 * 判断top2是否远大于其他值
	 */
	private static double outstandingTop2PValue(Double[] data, double beta) {
		int len = data.length;
		double max = Double.NEGATIVE_INFINITY;
		double max2 = Double.NEGATIVE_INFINITY;
		double sumT = 0;

		for (Double datum : data) {
			if (datum < max2) {
			} else if (datum > max2 && datum < max) {
				max2 = datum;
			} else {
				max2 = max;
				max = datum;
			}
			sumT += datum;
		}

		double[] dataReMax = new double[len - 2];
		double sum = 0;
		double sum2 = 0;
		int idx = 0;
		for (Double datum : data) {
			if (max != datum && max2 != datum) {
				dataReMax[idx] = datum;
				idx++;
				sum += datum;
				sum2 += datum * datum;
			}
		}

		if (idx == 0) {
			return 0;
		}
		if (idx < len - 3) {
			return 0;
		}

		double mu = sum / (len - 1);
		double sigma = Math.sqrt(Math.max(0.0, (sum2 - sum * sum / (len - 1)) / (len - 2)));

		Arrays.sort(dataReMax);

		List <Row> lrData = new ArrayList <>();
		for (int i = 0; i < len - 2; i++) {
			lrData.add(Row.of(Math.pow(len - 2 - i, -beta), dataReMax[i]));
		}

		//new MemSourceLocalOp(lrData, "x double, y double").print();

		String[] colNames = new String[] {"x", "y"};
		WindowTable wt = new WindowTable(colNames,
			new Class[] {Double.class, Double.class},
			lrData);

		SummaryResultTable srt = SrtUtil.batchSummary(wt, colNames,
			1, 1, 1, 1, StatLevel.L3);

		com.alibaba.alink.operator.common.regression.LinearRegressionModel lrModel =
			LinearReg.train(srt, colNames[1], new String[] {colNames[0]});

		double xMaxPred = lrModel.beta[0] + lrModel.beta[1] * Math.pow(len, -beta);
		//System.out.println("xMaxPred: " + xMaxPred);

		try {
			NormalDistribution distribution = new NormalDistribution(mu, sigma);
			return distribution.cumulativeProbability(Math.abs(xMaxPred - max2));
		} catch (Exception ex) {
			if (mu == 0.0 && sigma == 0.0) {
				return 0;
			} else {
				System.out.println("data: " + data.length);
				for (int i = 0; i < data.length; i++) {
					System.out.print(data[i]);
					System.out.print(",");
				}
				System.out.println();
			}
			throw new AkIllegalOperatorParameterException(
				"Input parameter out of range! mu: " + mu + " sigma: " + sigma);
		}
	}

	/**
	 * ref: https://www.microsoft.com/en-us/research/uploads/prod/2016/12/Insight-Types-Specification.pdf
	 */
	protected static double changePointScore(Double[] vals, int fromId, int toId, int changePointId) {
		Tuple3 <Double, Double, Integer> t3Left = getSumAndSum2(vals, fromId, changePointId);
		Tuple3 <Double, Double, Integer> t3Right = getSumAndSum2(vals, changePointId, toId);

		double cntLeft = t3Left.f2;
		double sumLeft = t3Left.f0;
		double sum2Left = t3Left.f1;
		double cntRight = t3Right.f2;
		double sumRight = t3Right.f0;
		double sum2Right = t3Right.f1;

		double muLeft = sumLeft / cntLeft;
		double muRight = sumRight / cntRight;
		double sigma2 = Math.sqrt((sum2Left + sum2Right) / (cntLeft + cntRight)
			- Math.pow((sumLeft + sumRight) / (cntLeft + cntRight), 2)) / Math.sqrt((cntLeft + cntRight) / 2);
		double kmean = Math.abs(muLeft - muRight) / sigma2;

		//System.out.println("kmean: " + kmean + " p: " + CDF.stdNormal(kmean));
		return CDF.stdNormal(kmean);

	}

	/**
	 * find change point using binary tree.
	 */
	protected static Integer[] findAllChangePointId(Double[] vals) {
		int len = vals.length;

		List <Integer> changePointIndices = new ArrayList <>();

		if (hasChangePoint(vals, 0, len)) {
			int idx = findChangePointId(vals, 0, len);
			changePointIndices.add(idx);
			int startId = 0;
			int endId = idx;
			int idx2 = 0;
			while (true) {
				while (hasChangePoint(vals, startId + 1, endId)) {
					idx2 = findChangePointId(vals, startId + 1, endId);
					changePointIndices.add(idx2);
					endId = idx2;
				}

				changePointIndices.sort(new Comparator <Integer>() {
					@Override
					public int compare(Integer o1, Integer o2) {
						return o1.compareTo(o2);
					}
				});

				startId = endId;
				int idx2idx = changePointIndices.indexOf(startId);
				if (idx2idx == changePointIndices.size() - 1) {
					endId = len;
				} else {
					endId = changePointIndices.get(idx2idx + 1);
				}

				if (startId >= endId) {
					break;
				}
			}

		}

		return changePointIndices.toArray(new Integer[0]);
	}

	/**
	 * Satterthwaite 近似T检验， H0: 两组样本均值相等。H1: 两组样本均值不等。
	 * 如果P < 0.05, 拒绝H0，接受H1, 差异具有统计学意义。
	 * ref: https://mengte.online/archives/327
	 */
	protected static double tTest(Double[] vals, int fromId, int toId, int splitPointId) {
		int len = vals.length;
		if (len < 20) {
			return 0;
		}

		Tuple3 <Double, Double, Integer> t3Left = getSumAndSum2(vals, fromId, splitPointId);
		Tuple3 <Double, Double, Integer> t3Right = getSumAndSum2(vals, splitPointId, toId);

		double cntLeft = t3Left.f2;
		double sumLeft = t3Left.f0;
		double sum2Left = t3Left.f1;
		double cntRight = t3Right.f2;
		double sumRight = t3Right.f0;
		double sum2Right = t3Right.f1;

		double muLeft = sumLeft / cntLeft;
		double muRight = sumRight / cntRight;
		double sigma2Left = (sum2Left - sumLeft * sumLeft / cntLeft) / (cntLeft - 1);
		double sigma2Right = (sum2Right - sumRight * sumRight / cntRight) / (cntRight - 1);

		double s1 = sigma2Left / cntLeft;
		double s2 = sigma2Right / cntRight;
		double df = (s1 + s2) * (s1 + s2) / (s1 * s1 / (cntLeft - 1) + s2 * s2 / (cntRight - 1));
		double tStatistic = (muLeft - muRight) / Math.sqrt(s1 + s2);
		double pValue = 2.0 * (1 - CDF.studentT(Math.abs(tStatistic), df));

		//System.out.println("df: " + df + " val: " + tStatistic + " pValue: " + pValue);

		return pValue;
	}

	protected static boolean hasChangePoint(Double[] vals, int fromId, int toId) {
		int len = toId - fromId;
		if (len < 20) {
			return false;
		}

		int k = fromId + (int) len / 2;

		double pValue = tTest(vals, fromId, toId, k);

		//System.out.println(
		//	"HasChangePoint: " + " fromId: " + fromId + " toId: " + toId + " k: " + k + " pValue: " + pValue);

		return pValue < 0.05;
	}

	/**
	 * ref: https://zhuanlan.zhihu.com/p/335322056
	 */
	private static int findChangePointId(Double[] vals, int fromId, int toId) {
		double minJ = Double.POSITIVE_INFINITY;
		int minId = 0;
		int k = 10;
		for (int i = fromId + k; i < toId; i++) {
			Tuple3 <Double, Double, Integer> t3Left = getSumAndSum2(vals, fromId, i);
			Tuple3 <Double, Double, Integer> t3Right = getSumAndSum2(vals, i, toId);

			double cntLeft = t3Left.f2;
			double sumLeft = t3Left.f0;
			double sum2Left = t3Left.f1;
			double cntRight = t3Right.f2;
			double sumRight = t3Right.f0;
			double sum2Right = t3Right.f1;

			double sigma2Left = (sum2Left - sumLeft * sumLeft / cntLeft) / (cntLeft - 1);
			double sigma2Right = (sum2Right - sumRight * sumRight / cntRight) / (cntRight - 1);

			double J = cntLeft * sigma2Left + cntRight * sigma2Right;
			if (J < minJ) {
				minJ = J;
				minId = i;
			}

		}

		return minId;
	}

	/**
	 * return <sum, sum2, count>
	 */
	private static Tuple3 <Double, Double, Integer> getSumAndSum2(Double[] vals, int fromId, int toId) {
		int cnt = 0;
		double sum = 0;
		double sum2 = 0;
		for (int i = Math.max(fromId, 0); i < Math.min(toId, vals.length); i++) {
			cnt++;
			sum += vals[i];
			sum2 += vals[i] * vals[i];
		}
		return Tuple3.of(sum, sum2, cnt);
	}

	/**
	 * /**
	 * * 幂律分布：ordinary long-tail distribution.
	 * * 如果一个数据分布符合幂律分布，那么其自变量的指数和因变量是线性的。那么问题转换为是否符合线性关系的假设检验。
	 * * 这是 x 是 i^{-beta}, y 是 data/max{data}, p value就是这个线性回归的 p value。
	 * * ref: https://www.jianshu.com/p/8b2003dcfce0
	 */
	protected static double outstandingNo1PValue(Double[] data, double beta, double max) {
		int maxFreq = 0;
		for (double val : data) {
			if (max == val) {
				maxFreq++;
			}
		}

		// for the same value. eg: 1,1,1,1,1
		if (maxFreq > 1) {
			return 0;
		}

		Tuple3 <Double, Double, Double[]> t3 = muAndSigmaExcludingFirstValue(data, max);

		double mu = t3.f0;
		double sigma = t3.f1;

		Double[] y = t3.f2;
		Double[] x = new Double[y.length];
		Double[] vals = new Double[y.length + 1];
		for (int i = 1; i < data.length; i++) {
			x[i - 1] = Math.pow(i + 1, -beta);
			vals[i - 1] = x[i - 1];
		}
		vals[y.length] = 1.0;

		Double[] xPred = linearRegression1D(x, y, vals);
		Tuple2 <Double, Double> tuple2 = muAndSigma(y, xPred);

		double sigmaResidual = tuple2.f1;

		// for same value except max. eg. 6,5,5,5,5,5
		if (sigmaResidual == 0) {
			sigmaResidual = 1.0;
		}

		try {
			return (1 - CDF.normal(xPred[y.length] - max, 0, sigmaResidual)) * 0.99;
		} catch (Exception ex) {
			throw new AkIllegalOperatorParameterException(
				"Input parameter out of range! mu: " + mu + " sigma: " + sigma);
		}
	}

	/**
	 * return < mu, sigma >
	 */
	protected static Tuple2 <Double, Double> muAndSigma(Double[] y, Double[] yPred) {
		int len = y.length;

		double sum = 0;
		double sum2 = 0;

		for (int i = 0; i < len; i++) {
			double res = yPred[i] - y[i];
			sum += res;
			sum2 += res * res;
		}

		double mu = sum / len;
		double sigma = Math.sqrt(Math.max(0.0, (sum2 - sum * sum / len) / (len - 1)));

		//System.out.println("mu: " + mu + " sigma: " + sigma);

		return Tuple2.of(mu, sigma);
	}

	/**
	 * return < mu, sigma, data\val >
	 */
	protected static Tuple3 <Double, Double, Double[]> muAndSigmaExcludingFirstValue(Double[] data, double val) {
		int len = data.length;

		Double[] dataReMax = new Double[len - 1];
		int idx = 0;
		double sum = 0;
		double sum2 = 0;
		for (Double datum : data) {
			if (val != datum) {
				dataReMax[idx] = datum;
				sum += datum;
				sum2 += datum * datum;
				idx++;
			} else {
				//System.out.println("===");
			}
		}

		double mu = sum / (len - 1);
		double sigma = Math.sqrt(Math.max(0.0, (sum2 - sum * sum / (len - 1)) / (len - 2)));

		try {
			Arrays.sort(dataReMax, Collections.reverseOrder());
		} catch (Exception ex) {
			System.out.println();
		}

		return Tuple3.of(mu, sigma, dataReMax);
	}

	/**
	 * ref: https://zhuanlan.zhihu.com/p/506802623
	 */
	protected static Double[] linearRegression1D(Double[] x, Double[] y, Double[] xForPred) {
		double sumXY = 0;
		double sumX = 0;

		double xMean = 0;
		double yMean = 0;
		for (int i = 0; i < x.length; i++) {
			xMean += x[i];
			yMean += y[i];
		}
		xMean = xMean / x.length;
		yMean = yMean / y.length;

		for (int i = 0; i < x.length; i++) {
			sumXY += (x[i] - xMean) * (y[i] - yMean);
			sumX += (x[i] - xMean) * (x[i] - xMean);
		}
		if (sumX == 0) {
			throw new AkIllegalStateException("x is the same value.");
		}
		double w = sumXY / sumX;
		double b = yMean - w * xMean;

		Double[] res = new Double[xForPred.length];
		for (int i = 0; i < xForPred.length; i++) {
			res[i] = w * xForPred[i] + b;
		}
		return res;
	}

	/**
	 * use chiSquare test for uniform distribution.
	 * ref: https://blog.csdn.net/weixin_39894778/article/details/111362337
	 */
	private static double chiSquare(List <Double> values, double mean) {
		double chiSq = 0;
		for (double val : values) {
			chiSq += (val - mean) * (val - mean) / mean;
		}
		int df = values.size() - 1;
		ChiSquaredDistribution distribution = new ChiSquaredDistribution(null, df);
		return 1.0 - distribution.cumulativeProbability(Math.abs(chiSq));
	}

	private static String createFilterClause(Subspace subspace) {
		if (subspace.value instanceof String) {
			return "`" + subspace.colName + "`='" + subspace.value + "'";
		} else {
			return "`" + subspace.colName + "`=" + subspace.value;
		}
	}

	private static List <Double> loadData(List <Row> rows, int idx) {
		List <Double> list = new ArrayList <>();
		for (Row row : rows) {
			if (null == row.getField(idx)) {
				continue;
			}
			list.add(((Number) row.getField(idx)).doubleValue());
		}
		return list;
	}

	private static String objToString(Object obj) {
		return obj == null ? "null" : obj.toString();

	}

}
