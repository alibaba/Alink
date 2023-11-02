package com.alibaba.alink.common.insights;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.linalg.DenseVector;
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
import com.alibaba.alink.params.statistics.HasStatLevel_L1;
import com.alibaba.alink.params.statistics.HasStatLevel_L1.StatLevel;
import com.alibaba.alink.pipeline.regression.LinearRegression;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.sql.Timestamp;
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
			System.out.println("filter sql: " + sbd.toString());
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

		dataAggr.lazyPrint("------ agg -------- ");

		// set layout data.
		Insight insight = calcInsight(subject, dataAggr, type);
		LayoutData layoutData = new LayoutData();
		layoutData.data = dataAggr.getOutputTable();
		insight.layout = layoutData;

		return insight;
	}

	public static Insight calcInsight(Subject subject, LocalOperator <?> dataAggr, InsightType type) {
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = type;

		switch (type) {
			case OutstandingNo1:
				insight.score = outstandingNo1(dataAggr, subject);
				break;
			case OutstandingLast:
				insight.score = outstandingNoLast(dataAggr, subject);
				break;
			case OutstandingTop2:
				insight.score = outstandingTop2(dataAggr, subject);
				break;
			case Evenness:
				insight.score = even(dataAggr, subject);
				break;
			case Attribution:
				insight.score = attribution(dataAggr, subject);
				break;
			case ChangePoint:
				insight.score = changePoint(dataAggr, subject);
				break;
			case Outlier:
				insight.score = outlier(dataAggr, subject);
				break;
			case Trend:
				insight.score = trend(dataAggr, subject);
				break;
			case Seasonality:
				insight.score = seasonality(dataAggr, subject);
				break;
			default:
				throw new AkIllegalOperatorParameterException("Insight type not support yet!" + type.name());
		}

		return insight;
	}

	private static double outstandingNo1(LocalOperator <?> dataAggr, Subject subject) {
		if (subject.measures.size() != 1) {
			return 0;
		}

		String measureCol = MEASURE_NAME_PREFIX + "0";

		TableSummary summary = dataAggr.select(measureCol).collectStatistics();

		// 一个值，没有必要； 两个值，直接比较；三个值，去掉最大值，只剩两个，那么残差必然是0，所以至少要有4个点
		long rowNum = summary.count();
		if (rowNum == 0 || rowNum == 1) {
			return 0;
		}

		if (summary.min(measureCol) == summary.max(measureCol)) {
			return 0;
		}

		List <Double> values = new ArrayList <>();
		for (Row row : dataAggr.select(measureCol).getOutputTable().getRows()) {
			values.add(((Number) row.getField(0)).doubleValue());
		}

		if (rowNum == 2) {
			return 0;
		}
		if (rowNum == 3) {
			return summary.maxDouble(measureCol) / summary.sum(measureCol);
		}

		double max = summary.maxDouble(measureCol);
		if (max / summary.sum(measureCol) < 0.1) {
			return 0;
		}
		return outstandingNo1PValue(values.toArray(new Double[0]), 0.7, max);
	}

	private static double outstandingNoLast(LocalOperator <?> dataAggr, Subject subject) {
		if (subject.measures.size() != 1) {
			return 0;
		}

		String measureCol = MEASURE_NAME_PREFIX + "0";

		TableSummary summary = dataAggr.select(measureCol).collectStatistics();

		// 一个值，没有必要； 两个值，直接比较；三个值，去掉最大值，只剩两个，那么残差必然是0，所以至少要有4个点
		long rowNum = summary.count();
		if (rowNum == 0 || rowNum == 1) {
			return 0;
		}

		if (summary.maxDouble(measureCol) > 0) {
			return 0;
		}

		if (summary.min(measureCol) == summary.max(measureCol)) {
			return 0;
		}

		List <Double> values = new ArrayList <>();
		for (Row row : dataAggr.select(measureCol).getOutputTable().getRows()) {
			values.add(-((Number) row.getField(0)).doubleValue());
		}

		if (rowNum == 2) {
			return Math.abs(summary.minDouble(measureCol)) / (Math.abs(values.get(0)) + Math.abs(values.get(1)));
		}

		if (rowNum == 3) {
			return Math.abs(summary.minDouble(measureCol)) /
				(Math.abs(values.get(0)) + Math.abs(values.get(1)) + Math.abs(values.get(2)));
		}

		return outstandingNo1PValue(values.toArray(new Double[0]), 0.7, Math.abs(summary.minDouble(measureCol)));
	}

	private static double outstandingTop2(LocalOperator <?> dataAggr, Subject subject) {
		if (subject.measures.size() != 1) {
			return 0;
		}

		String measureCol = MEASURE_NAME_PREFIX + "0";

		TableSummary summary = dataAggr.select(measureCol).collectStatistics();

		if (summary.min(measureCol) == summary.max(measureCol)) {
			return 0;
		}
		long rowNum = summary.count();

		// rowNum =2时，必然有一个值大于等于50%
		if (rowNum <= 2) {
			return 0;
		}

		double max = Double.NEGATIVE_INFINITY;
		double max2 = Double.NEGATIVE_INFINITY;
		double sumT = 0;

		List <Double> values = new ArrayList <>();
		for (Row row : dataAggr.select(measureCol).getOutputTable().getRows()) {
			values.add(((Number) row.getField(0)).doubleValue());
		}

		Double[] vals = values.toArray(new Double[0]);
		for (Double datum : vals) {
			if (datum < max2) {
			} else if (datum > max2 && datum < max) {
				max2 = datum;
			} else {
				max2 = max;
				max = datum;
			}
			sumT += datum;
		}

		if (max / sumT >= 0.5) {
			return 0;
		}
		if ((max + max2) < 0.5) {
			return 0;
		}

		if (rowNum <= 4) {
			return (max + max2) / summary.sum(measureCol);
		}

		if ((max + max2) / summary.sum(measureCol) < 0.1) {
			return 0;
		}

		return outstandingTop2PValue(vals, 0.7);
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

		if (max / sumT >= 0.5) {
			return 0;
		}
		if ((max + max2) < 0.5) {
			return 0;
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
			} else {
				//System.out.println("===");
			}
		}

		if (idx == 0) {
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
			throw new AkIllegalOperatorParameterException(
				"Input parameter out of range! mu: " + mu + " sigma: " + sigma);
		}
	}

	private static double attribution(LocalOperator <?> dataAggr, Subject subject) {
		if (subject.measures.size() != 1) {
			return 0;
		}

		String measureCol = MEASURE_NAME_PREFIX + "0";

		TableSummary summary = dataAggr.select(measureCol).collectStatistics();
		double max = summary.maxDouble(measureCol);
		double sum = summary.sum(measureCol);
		if (max / sum > 0.5) {
			return outstandingNo1(dataAggr, subject) * 1.001;
		} else {
			return 0;
		}
	}

	/**
	 * 变点的前k个值和后k个值，是两个不同的分布。使用独立性T检验判断，找出所有的change point。再计算最大的score。
	 */
	private static double changePoint(LocalOperator <?> dataAggr, Subject subject) {
		if (subject.measures.size() != 1) {
			return 0;
		}

		MTable mt = dataAggr.getOutputTable();
		mt.orderBy(subject.breakdown.colName);

		List <Double> values = new ArrayList <>();
		for (Row row : mt.getRows()) {
			values.add(((Number) row.getField(1)).doubleValue());
		}

		Double[] vals = values.toArray(new Double[0]);

		Integer[] changePointIndices = findAllChangePointId(vals);

		if (changePointIndices.length == 0) {
			return 0;
		}

		double scoreMax = 0;
		int k = 20;
		for (int i = 0; i < changePointIndices.length; i++) {
			//int startIdx = i == 0 ? 0 : changePointIndices[i - 1];
			//int endIdx = i == changePointIndices.length - 1 ? vals.length : changePointIndices[i + 1];
			int startIdx = Math.max(0, changePointIndices[i] - k);
			int endIdx = Math.min(vals.length, changePointIndices[i] + k);
			double score = changePointScore(vals, startIdx, endIdx, changePointIndices[i]);
			if (score > scoreMax) {
				scoreMax = score;
			}
		}

		return scoreMax;
	}

	/**
	 * KSigma Outlier.
	 */
	private static double outlier(LocalOperator <?> dataAggr, Subject subject) {
		if (subject.measures.size() != 1) {
			return 0;
		}

		String measureCol = MEASURE_NAME_PREFIX + "0";

		MTable mt = dataAggr.getOutputTable();
		mt.orderBy(subject.breakdown.colName);

		double[] values = new double[mt.getNumRow()];
		int idx = 0;
		for (Row row : mt.getRows()) {
			values[idx] = ((Number) row.getField(1)).doubleValue();
			idx++;
		}

		double[] zScores = values.clone();

		TableSummary summary = dataAggr.select(measureCol).collectStatistics();
		final double mean = summary.mean(measureCol);
		final double standardDeviation = summary.standardDeviation(measureCol);

		for (int i = 0; i < zScores.length; i++) {
			if (standardDeviation != 0.0) {
				zScores[i] = (zScores[i] - mean) / standardDeviation;
			} else {
				zScores[i] = 0.0;
			}
		}

		//System.out.println("zScore: " + new DenseVector(zScores));

		double max = Double.NEGATIVE_INFINITY;
		for (double score : zScores) {
			max = Math.max(max, Math.abs(score));
		}

		return 1 - 2 * CDF.stdNormal(-max);
	}

	/**
	 * H0: 没有趋势; H1: 有升或者降趋势.
	 * ref: Extrating Top-K Insights from Multi-dimensional Data.
	 * https://zhuanlan.zhihu.com/p/112703276?utm_id=0
	 */
	private static double trend(LocalOperator <?> dataAggr, Subject subject) {
		MTable mt = dataAggr.getOutputTable();
		mt.orderBy(subject.breakdown.colName);

		String measureCol = MEASURE_NAME_PREFIX + "0";

		LocalOperator <?> data = new MemSourceLocalOp(mt)
			.link(new AppendIdLocalOp().setIdCol("__alink_id__"))
			.select("__alink_id__, " + measureCol);

		String[] colNames = new String[] {"x", "y"};
		Class[] colTypes = new Class[] {Double.class, Double.class};

		WindowTable wt = new WindowTable(colNames, colTypes, data.collect());
		SummaryResultTable srt = SrtUtil.batchSummary(wt, colNames,
			1, 1, 1, 1, StatLevel.L3);

		LinearRegressionModel lrModel = LinearReg.train(srt, colNames[0], new String[] {colNames[1]});
		double r2 = lrModel.R2;
		double slope = lrModel.beta[1];

		double p = 1 - 1 / (1 + Math.exp(-(slope - 0.2) / 2));

		//System.out.println("r2: " + r2 + " slope: " + slope + " p: " + p);

		return r2 * (1 - p);
	}

	/**
	 * pattern presents the repeated pattern in a time series.
	 * 按照多个序列计算相关系数的方式: ACF
	 * ref: https://www.microsoft.com/en-us/research/uploads/prod/2021/03/metainsight-extended.pdf
	 * https://www.microsoft.com/en-us/research/uploads/prod/2016/12/Insight-Types-Specification.pdf
	 * https://zhuanlan.zhihu.com/p/93186317?utm_source=qq
	 */

	private static double seasonality(LocalOperator <?> dataAggr, Subject subject) {
		if (subject.measures.size() != 1) {
			return 0;
		}

		String measureCol = MEASURE_NAME_PREFIX + "0";

		MTable mt = dataAggr.getOutputTable();
		mt.orderBy(subject.breakdown.colName);

		double[] values = new double[mt.getNumRow()];
		int idx = 0;
		for (Row row : mt.getRows()) {
			values[idx] = ((Number) row.getField(1)).doubleValue();
			idx++;
		}

		ArrayList <double[]> acfAndConfidence = TsMethod.acf(values, Math.min(values.length / 2, 12));

		return 0;

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

		//System.out.print("changPoints: ");
		//for (Integer id : changePointIndices) {
		//	System.out.print(id + ", ");
		//}
		//System.out.println();

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

	private static double even(LocalOperator <?> dataAggr, Subject subject) {
		if (subject.measures.size() != 1) {
			return 0;
		}

		String measureCol = MEASURE_NAME_PREFIX + "0";

		TableSummary summary = dataAggr.collectStatistics();

		if (summary.count() < 5 || summary.min(measureCol) == summary.max(measureCol)) {
			return 0;
		}

		if (summary.count() > 20) {
			return 0;
		}

		double mean = summary.mean(measureCol);

		List <Double> values = new ArrayList <>();
		for (Row row : dataAggr.select(measureCol).getOutputTable().getRows()) {
			values.add(((Number) row.getField(0)).doubleValue());
		}

		return chiSquare(values, mean);
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
		} else if (subspace.value instanceof Timestamp) {
			Timestamp ts = (Timestamp) subspace.value;
			return String.format("unix_timestamp_macro(%s) = %s", subspace.colName, ts.getTime());
		} else {
			return "`" + subspace.colName + "`=" + subspace.value;
		}
	}
}
