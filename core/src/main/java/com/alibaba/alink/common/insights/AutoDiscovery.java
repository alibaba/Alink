package com.alibaba.alink.common.insights;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.operator.local.LocalOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.alibaba.alink.common.insights.Mining.MEASURE_NAME_PREFIX;

public class AutoDiscovery {

	public static List <Insight> find(LocalOperator <?> data) throws Exception {
		return find(data, Float.MAX_VALUE);
	}

	public static List <Insight> find(LocalOperator <?> data, float limitedSeconds) throws Exception {
		final long startTime = System.currentTimeMillis();
		final long stopTime = startTime + (long) (1000 * limitedSeconds);

		List <Insight> output = new ArrayList <>();

		System.out.println("start...........");

		long start = System.currentTimeMillis();

		//whole data
		if (!isTimeOut(stopTime)) {
			findInSingleSubspace(data, new ArrayList <>(), 1.0, stopTime, output, false, 0);
		}

		long end = System.currentTimeMillis();
		System.out.println("whole table single find time: " + (end - start) / 1000 + "s.");

		/**
		 * STEP 1 : Task Generation
		 */
		System.out.println("start task generation..................");
		start = System.currentTimeMillis();

		ImpactDetector impactDetector = new ImpactDetector(AutoDiscoveryConstants.IMPACT_THRESHOLD);
		impactDetector.detect(data);

		end = System.currentTimeMillis();
		System.out.println("subspace impact detect time: " + (end - start) / 1000 + "s.");

		/**
		 * STEP 2 : Evaluation in limited seconds
		 */
		//1-subspace
		if (!isTimeOut(stopTime)) {
			System.out.println("single sub space: " + impactDetector.listSingleSubspace().size());
			int i = 0;
			for (Tuple2 <Subspace, Double> t2 : impactDetector.listSingleSubspace()) {
				start = System.currentTimeMillis();

				findInSingleSubspace(data, Collections.singletonList(t2.f0),
					t2.f1, stopTime, output, false, 0);

				end = System.currentTimeMillis();
				System.out.println("subspace [" + i + "] single find time: " + (end - start) / 1000);
				i++;

				if (isTimeOut(stopTime)) {break;}
			}
		}

		// 1-subspace correlation
		System.out.println("start subspace correlation...............");
		long totalTime = 0;
		if (!isTimeOut(stopTime)) {
			List <Tuple2 <String, List <Tuple2 <Subspace, Double>>>> colList = impactDetector.listSubspaceByCol();
			System.out.println("total subspace cols " + colList.size());
			for (Tuple2 <String, List <Tuple2 <Subspace, Double>>> t : colList) {
				start = System.currentTimeMillis();
				if (t.f1.size() <= 1) {
					continue;
				}
				List <Subspace> subspaces = new ArrayList <>();
				for (Tuple2 <Subspace, Double> tuple : t.f1) {
					subspaces.add(tuple.f0);
				}

				BreakdownDetector detector = new BreakdownDetector().detect(data, subspaces, true,
					AutoDiscoveryConstants.BREAKDOWN_DISTINCT_COUNT_THRESHOLD, LocalOperator.getParallelism());

				int measureSize = 0;
				for (Tuple2 <Breakdown, List <Measure>> bdTuple : detector.list) {
					List <LocalOperator <?>> dataAgg = AggregationQuery.sameSubspaceColQuery(data, t.f0, subspaces,
						bdTuple.f0, bdTuple.f1);
					findInCrossSubspacesByBreakdown(t.f1, dataAgg, bdTuple.f0, bdTuple.f1, stopTime, output);
					measureSize += bdTuple.f1.size();
				}
				System.out.println("subspace col: " + t.f0 + " subspaces size: " +
					t.f1.size() + " breakdown size: " + detector.list.size() + " measure size:" + measureSize);
				System.out.println("subspace correlation use " + (System.currentTimeMillis() - start) + " ms");
				totalTime += (System.currentTimeMillis() - start);

				if (isTimeOut(stopTime)) {break;}
			}
		}
		System.out.println("subspace correlation use " + totalTime + " ms");

		while (!isTimeOut(stopTime)) {
			Thread.sleep(234);
		}

		/**
		 * STEP 3 : Recommendation
		 */
		output.sort(new Comparator <Insight>() {
			@Override
			public int compare(Insight o1, Insight o2) {
				return -Double.compare(o1.score, o2.score);
			}
		});
		return output;
	}

	static void basicStat(LocalOperator <?> data, final long stopTime, List <Insight> output) {
		basicStat(data, data.getColNames(), stopTime, output);
	}

	static void basicStat(LocalOperator <?> data, String[] selectColNames, final long stopTime,
						  List <Insight> output) {
		for (String colName : selectColNames) {
			Insight insight = StatInsight.basicStat(data, colName);
			output.add(insight);
			if (isTimeOut(stopTime)) {break;}
		}
	}

	public static void findInSingleSubspace(LocalOperator <?> data,
											List <Subspace> subspaces,
											Breakdown breakdown,
											List <Measure> measures,
											double impact,
											long stopTime,
											List <Insight> outInsights,
											boolean isSingleThread,
											int threadId
	) {
		LocalOperator <?> curData = data;
		if (!subspaces.isEmpty()) {
			curData = Mining.filter(data, subspaces);
		}

		if (AutoDiscovery.isTimeOut(stopTime)) {return;}

		List <LocalOperator <?>> dataAggrs = AggregationQuery.query(curData, breakdown, measures, isSingleThread);

		if (AutoDiscovery.isTimeOut(stopTime)) {return;}

		if (subspaces.isEmpty()) {
			StatInsight.distributionMulti(dataAggrs, breakdown, measures, outInsights);
		}


		System.out.println(
			String.format("threadId[%s], subspace[%s] breakdown[%s] distinctCount: %s measures.size(): %s",
				threadId,
				subspaces.isEmpty() ? "" : subspaces.get(0),
				breakdown.colName, dataAggrs.get(0).getOutputTable().getNumRow(), measures.size()));

		boolean isTimestampCol = AutoDiscovery.isTimestampCol(data.getSchema(), breakdown.colName);
		CorrelationInsightBase correlationInsightBase;
		for (int iMeas = 0; iMeas < measures.size(); iMeas++) {
			if (AutoDiscovery.isTimeOut(stopTime)) {break;}
			Subject subject = new Subject()
				.setSubspaces(subspaces)
				.setBreakdown(breakdown)
				.addMeasure(measures.get(iMeas));
			LocalOperator <?> dataAggr = dataAggrs.get(iMeas);

			if (isTimestampCol) {
				for (InsightType type : Insight.singleShapeInsightType()) {
					try {
						Insight insight = Mining.calcInsight(subject, dataAggr, type);
						insight.score *= impact;

						if (insight.score >= AutoDiscoveryConstants.SCORE_THRESHOLD) {
							outInsights.add(insight);
						}
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			} else {
				for (InsightType type : Insight.singlePointInsightType()) {
					try {
						Insight insight = Mining.calcInsight(subject, dataAggr, type);
						insight.score *= impact;

						if (insight.score >= AutoDiscoveryConstants.SCORE_THRESHOLD) {
							outInsights.add(insight);
						}
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			}

			if (AutoDiscovery.isTimeOut(stopTime)) {break;}

			for (int jMeas = iMeas + 1; jMeas < measures.size(); jMeas++) {
				if (AutoDiscovery.isTimeOut(stopTime)) {break;}
				// 不计算measure列相同的数据
				if (measures.get(iMeas).colName.equals(measures.get(jMeas).colName)) {
					continue;
				}
				LocalOperator <?> dataAggr1 = dataAggrs.get(jMeas);
				Subject correlationSubject =
					new Subject()
						.setSubspaces(subspaces)
						.setBreakdown(breakdown)
						.addMeasure(measures.get(iMeas))
						.addMeasure(measures.get(jMeas));
				for (InsightType type : Insight.measureCorrInsightType()) {
					try {
						if (AutoDiscovery.isTimeOut(stopTime)) {break;}
						Insight insight = new Insight();
						insight.subject = correlationSubject;
						insight.type = type;

						correlationInsightBase = MiningInsightFactory.getMiningInsight(insight);
						correlationInsightBase.setSingleThread(isSingleThread);
						correlationInsightBase.processData(dataAggr, dataAggr1);
						insight.score *= impact;
						if (insight.score >= AutoDiscoveryConstants.SCORE_THRESHOLD) {
							outInsights.add(insight);
						}
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
				if (AutoDiscovery.isTimeOut(stopTime)) {break;}
			}

			if (AutoDiscovery.isTimeOut(stopTime)) {break;}
		}
	}

	public static void findInCrossSubspacesByBreakdown(List <Tuple2 <Subspace, Double>> subspaces,
													   List <LocalOperator <?>> dataAgg,
													   Breakdown breakdown,
													   List <Measure> measures,
													   final long stopTime,
													   List <Insight> output) {
		int measureIdx = 0;
		for (Measure m : measures) {
			for (int i = 0; i < subspaces.size() - 1; i++) {
				for (int j = i + 1; j < subspaces.size(); j++) {
					Subject subject = new Subject();
					subject.addMeasure(m).addSubspace(subspaces.get(i).f0).setBreakdown(breakdown);
					Insight insight = new Insight();
					insight.subject = subject;
					insight.type = InsightType.Correlation;
					insight.addAttachSubspace(subspaces.get(j).f0);
					CorrelationInsight correlationInsight = new CorrelationInsight(insight);
					String[] columns = new String[] {breakdown.colName, MEASURE_NAME_PREFIX + measureIdx};
					correlationInsight.processData(dataAgg.get(i).select(columns), dataAgg.get(j).select(columns));
					insight.score *= (subspaces.get(i).f1 + subspaces.get(j).f1);
					if (insight.score >= AutoDiscoveryConstants.SCORE_THRESHOLD) {
						output.add(insight);
					}
				}
			}
			measureIdx++;
			if (isTimeOut(stopTime)) {break;}
		}
	}

	static void findInSingleSubspace(LocalOperator <?> data,
									 List <Subspace> subspaces,
									 double impact,
									 final long stopTime,
									 List <Insight> output,
									 boolean isSingleThread,
									 int threadId
	) {
		long start = System.currentTimeMillis();
		if (impact < AutoDiscoveryConstants.SCORE_THRESHOLD) {
			return;
		}

		if (null != subspaces && subspaces.size() > 0) {
			data = Mining.filter(data, subspaces);
		}

		long end = System.currentTimeMillis();
		System.out.println("filter: " + (end - start) / 1000 + "s.");

		start = System.currentTimeMillis();

		BreakdownDetector breakdownDetector = new BreakdownDetector()
			.detect(data, new ArrayList <>(), false,
				AutoDiscoveryConstants.BREAKDOWN_DISTINCT_COUNT_THRESHOLD, LocalOperator.getParallelism());

		end = System.currentTimeMillis();

		System.out.println("breakdown detect:" + (end - start) / 1000 + "s.");
		System.out.println("breakdown size: " + breakdownDetector.list.size());

		if (subspaces.isEmpty()) {
			start = System.currentTimeMillis();
			basicStat(data, stopTime, output);
			end = System.currentTimeMillis();
			System.out.println("basic statics time:" + (end - start) / 1000 + "s.");
		}

		for (Tuple2 <Breakdown, List <Measure>> t2 : breakdownDetector.list) {
			AutoDiscovery.findInSingleSubspace(data, subspaces, t2.f0, t2.f1,
				impact, stopTime, output, isSingleThread, threadId);
			if (isTimeOut(stopTime)) {break;}
		}

	}

	public static boolean isTimestampCol(TableSchema schema, String colName) {
		return Types.SQL_TIMESTAMP == schema.getFieldType(colName).get();
	}

	public static boolean isTimeOut(long stopTime) {
		long curTime = System.currentTimeMillis();
		//if (curTime > stopTime) {
		//	System.out.println("Time is up.");
		//} else {
		//	System.out.println("Remaining time : " + (stopTime - System.currentTimeMillis()) / 1000.0 + " seconds.");
		//}
		return curTime > stopTime;
	}

}
