package com.alibaba.alink.common.insights;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.operator.local.LocalOperator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class AutoDiscovery {

	static double SCORE_THRESHOLD = 0.01;

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
			findInSubspaces(data, new ArrayList <>(), 1.0, stopTime, output);
		}

		long end = System.currentTimeMillis();
		System.out.println("whole table time: " + (end - start) / 1000);
		/**
		 * STEP 1 : Task Generation
		 */

		System.out.println("start task generation..................");
		start = System.currentTimeMillis();

		ImpactDetector impactDetector = new ImpactDetector(0.03);
		impactDetector.detect(data);

		end = System.currentTimeMillis();
		System.out.println("detect time: " + (end - start) / 1000);

		System.out.println("begin test---------------");
		/**
		 * STEP 2 : Evaluation in limited seconds
		 */
		//1-subspace
		if (!isTimeOut(stopTime)) {
			System.out.println("single sub space: " + impactDetector.listSingleSubspace().size());
			int i = 0;
			for (Tuple2 <Subspace, Double> t2 : impactDetector.listSingleSubspace()) {
				start = System.currentTimeMillis();
				List <Subspace> subspaces = new ArrayList <>();
				subspaces.add(t2.f0);
				findInSubspaces(data, subspaces, t2.f1, stopTime, output);
				end = System.currentTimeMillis();
				System.out.println("subspace [" + i + "] find time: " + (end - start) / 1000);
				i++;
			}
		}

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

	static void findInSubspaces(LocalOperator <?> data, List <Subspace> subspaces,
								double impact, final long stopTime, List <Insight> output
	) {
		long start = System.currentTimeMillis();
		long end = 0;
		System.out.println("start find...........");
		if (impact < SCORE_THRESHOLD) {
			return;
		}

		if (null != subspaces && subspaces.size() > 0) {
			data = Mining.filter(data, subspaces);
		}

		end = System.currentTimeMillis();
		System.out.println("filter: " + (end - start) / 1000);

		System.out.println("start find...........");
		BreakdownDetector breakdownDetector = new BreakdownDetector().detect(data, new ArrayList <>());
		end = System.currentTimeMillis();
		System.out.println("detect:" + (end - start) / 1000 + "s.");
		System.out.println(" breakdown size: " + breakdownDetector.list.size());

		for (Tuple2 <Breakdown, List <Measure>> t2 : breakdownDetector.list) {
			Breakdown breakdown = t2.f0;
			List <Measure> measures = t2.f1;

			System.out.println("breakdown[" + t2.f0.colName + "]  measures.size(): " + measures.size());

			start = System.currentTimeMillis();
			List <LocalOperator <?>> dataAggrs = AggregationQuery.query(data, breakdown, measures);

			end = System.currentTimeMillis();
			System.out.println("agg.........." + (end - start) / 1000);

			boolean isTimestampCol = isTimestampCol(data.getSchema(), breakdown.colName);

			CorrelationInsightBase correlationInsightBase;
			for (int i = 0; i < measures.size(); i++) {
				LocalOperator <?> dataAggr = dataAggrs.get(i);
				Subject subject = new Subject().setSubspaces(subspaces).setBreakdown(breakdown).addMeasure(
					measures.get(i));

				if (isTimestampCol) {
					for (InsightType type : Insight.singleShapeInsightType()) {
						try {
							Insight insight = Mining.calcInsight(subject, dataAggr, type);
							insight.score *= impact;

							if (insight.score >= SCORE_THRESHOLD) {
								LayoutData layoutData = new LayoutData();
								layoutData.data = dataAggr.getOutputTable();
								insight.layout = layoutData;

								output.add(insight);
							}
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					}
					end = System.currentTimeMillis();
					//System.out.println("single shape run time: " + (end - start) / 1000);
				} else {
					start = System.currentTimeMillis();
					for (InsightType type : Insight.singlePointInsightType()) {
						try {
							Insight insight = Mining.calcInsight(subject, dataAggr, type);
							insight.score *= impact;

							if (insight.score >= SCORE_THRESHOLD) {
								LayoutData layoutData = new LayoutData();
								layoutData.data = dataAggr.getOutputTable();
								insight.layout = layoutData;

								output.add(insight);
							}
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					}
					end = System.currentTimeMillis();
					//System.out.println("single point run time: " + (end - start) / 1000);
				}

				start = System.currentTimeMillis();
				// 遍历同一个subspace，两个measure的insight
				for (int j = i + 1; j < measures.size(); j++) {
					// 不计算measure列相同的数据
					if (measures.get(i).colName.equals(measures.get(j).colName)) {
						continue;
					}
					LocalOperator <?> dataAggr1 = dataAggrs.get(j);
					Subject correlationSubject = new Subject().setSubspaces(subspaces).setBreakdown(breakdown)
						.addMeasure(
							measures.get(i)).addMeasure(measures.get(j));
					for (InsightType type : new InsightType[] {InsightType.CrossMeasureCorrelation, InsightType
						.Clustering2D}) {
						try {
							correlationInsightBase = MiningInsightFactory.getMiningInsight(type, correlationSubject);
							Insight insight = correlationInsightBase.processData(dataAggr, dataAggr1);
							insight.score *= impact;
							if (insight.score >= SCORE_THRESHOLD) {
								output.add(insight);
							}
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					}
				}
				end = System.currentTimeMillis();
				//System.out.println("cross run time: " + (end - start) / 1000);
			}

			if (isTimeOut(stopTime)) {break;}
		}
	}

	public static boolean isTimestampCol(TableSchema schema, String colName) {
		return Types.SQL_TIMESTAMP == schema.getFieldType(colName).get();
	}

	public static boolean isTimeOut(long stopTime) {
		long curTime = System.currentTimeMillis();
		if (curTime > stopTime) {
			System.out.println("Time is up.");
		} else {
			System.out.println("Remaining time : " + (stopTime - System.currentTimeMillis()) / 1000.0 + " seconds.");
		}
		return curTime > stopTime;
	}

	static void taskGeneration(LocalOperator <?> data) {
	}

	static void evaluation() {}

	static void recommendation() {}
}
