package com.alibaba.alink.common.insights;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class AutoDiscoveryMultiThread {

	public static List <Insight> find(LocalOperator <?> data, float limitedSeconds) throws Exception {
		// get parallelism.
		int parallelism = LocalOperator.getParallelism();

		int crossMeasureThreadNum = (int) Math.ceil(parallelism * AutoDiscoveryConstants.crossMeasureThreadRatio);
		int singleMeasureThreadNum = parallelism - crossMeasureThreadNum;

		System.out.println("crossMeasureThreadNum: " + crossMeasureThreadNum);
		System.out.println("singleMeasureThreadNum: " + singleMeasureThreadNum);

		final long startTime = System.currentTimeMillis();
		final long stopTime = startTime + (long) (1000 * limitedSeconds);

		// find all <subspace, breakdown>, including one step;
		long startT = System.currentTimeMillis();
		Tuple2 <List <Tuple4 <List <Subspace>, Breakdown, List <Measure>, Double>>,
			List <Tuple2 <String, List <Tuple2 <Subspace, Double>>>>> searchSpaces =
			findSearchSpace(data, BreakdownDetector.getBreakdownCols(data.getSchema()),
				AutoDiscoveryConstants.IMPACT_THRESHOLD,
				AutoDiscoveryConstants.BREAKDOWN_DISTINCT_COUNT_THRESHOLD,
				parallelism);

		List <Tuple4 <List <Subspace>, Breakdown, List <Measure>, Double>> singleSearchSpaces = searchSpaces.f0;
		List <Tuple2 <String, List <Tuple2 <Subspace, Double>>>> colSearchSpaces = searchSpaces.f1;

		System.out.println("find run time: " + (System.currentTimeMillis() - startT) / 1000 + "s.");
		System.out.println("find size:" + singleSearchSpaces.size());

		List <Insight> outInsights = new CopyOnWriteArrayList <>();

		final TaskRunner taskRunner = new TaskRunner();

		for (int iThread = 0; iThread < parallelism; ++iThread) {
			if (iThread < singleMeasureThreadNum) {
				final int threadId = iThread;

				String[] colNames = data.getColNames();
				int colStep = colNames.length / singleMeasureThreadNum;

				int searchSpacesSize = singleSearchSpaces.size();
				int step = searchSpacesSize / singleMeasureThreadNum;
				int cnt = searchSpacesSize % singleMeasureThreadNum;

				System.out.println("searchSpacesSize: " + searchSpacesSize + " step: " + step + " cnt: " + cnt);

				taskRunner.submit(() -> {
						// for basic stat.
						for (int j = 0; j < colStep + 1; j++) {
							int idx = threadId + j * singleMeasureThreadNum;
							if (idx < colNames.length) {
								Insight insight = StatInsight.basicStat(data, colNames[idx]);
								outInsights.add(insight);
							}
						}

						// for single insight.
						for (int j = 0; j < step + 1; j++) {
							int idx = threadId + j * singleMeasureThreadNum;

							System.out.println("threadId[" + threadId + "] idx: " + idx);

							if (idx < searchSpacesSize) {
								long start1 = System.currentTimeMillis();

								Tuple4 <List <Subspace>, Breakdown, List <Measure>, Double> t4 =
									singleSearchSpaces.get(idx);
								AutoDiscovery.findInSingleSubspace(data, t4.f0, t4.f1, t4.f2, t4.f3, stopTime,
									outInsights, 1, threadId);

								long end1 = System.currentTimeMillis();
								System.out.println(String.format("threadId[" + threadId + "]" +
										"find in single subspace[%s] breakdown[%s] run time: %s",
									t4.f0.isEmpty() ? "" : t4.f0.get(0),
									t4.f1.colName,
									(end1 - start1) / 1000));
							}

							if (AutoDiscovery.isTimeOut(stopTime)) {break;}
						}
					}
				);
			} else {
				int crossTaskId = iThread - singleMeasureThreadNum;
				int searchSpacesSize = colSearchSpaces.size();
				final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(crossTaskId,
					crossMeasureThreadNum, searchSpacesSize);
				final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(crossTaskId,
					crossMeasureThreadNum, searchSpacesSize);

				if (cnt <= 0) {continue;}

				taskRunner.submit(() -> {
					for (int j = start; j < start + cnt; j++) {
						Tuple2 <String, List <Tuple2 <Subspace, Double>>> t = colSearchSpaces.get(j);
						if (t.f1.size() <= 1) {
							continue;
						}
						List <Subspace> subspaces = new ArrayList <>();
						for (Tuple2 <Subspace, Double> tuple : t.f1) {
							subspaces.add(tuple.f0);
						}

						long start1 = System.currentTimeMillis();

						BreakdownDetector detector = new BreakdownDetector().detect(data, subspaces, true,
							AutoDiscoveryConstants.BREAKDOWN_DISTINCT_COUNT_THRESHOLD, 1);

						long end1 = System.currentTimeMillis();
						System.out.println("cross subspace detect run time: " + (end1 - start1) / 1000 + "s.");
						start1 = System.currentTimeMillis();

						for (Tuple2 <Breakdown, List <Measure>> bdTuple : detector.list) {
							List <LocalOperator <?>> dataAgg = AggregationQuery.sameSubspaceColQuery(
								data, t.f0, subspaces, bdTuple.f0, bdTuple.f1, 1);
							AutoDiscovery.findInCrossSubspacesByBreakdown(t.f1, dataAgg, bdTuple.f0, bdTuple.f1,
								stopTime, outInsights, 1);
							if (AutoDiscovery.isTimeOut(stopTime)) {break;}
						}

						end1 = System.currentTimeMillis();
						System.out.println("cross subspace find run time: " + (end1 - start1) / 1000 + "s.");

						if (AutoDiscovery.isTimeOut(stopTime)) {break;}
					}
				});
			}
		}

		taskRunner.join();

		return InsightDecay.sortInsights(outInsights, 0.6);
	}

	public static Tuple2 <List <Tuple4 <List <Subspace>, Breakdown, List <Measure>, Double>>,
		List <Tuple2 <String, List <Tuple2 <Subspace, Double>>>>>
	findSearchSpace(LocalOperator <?> data, String[] breakdownCols,
					double impactThreshold, int breakDownThreshold, int threadNum) {
		List <Tuple4 <List <Subspace>, Breakdown, List <Measure>, Double>> subspaceAndBreakDown = new ArrayList <>();
		List <Tuple2 <String, List <Tuple2 <Subspace, Double>>>> subspaceAndBreakDownByCol;

		// for whole table
		{
			long start = System.currentTimeMillis();
			long end;

			BreakdownDetector breakdownDetector = new BreakdownDetector().detect(data, breakdownCols,
				new ArrayList <>(), false, breakDownThreshold, threadNum);

			for (Tuple2 <Breakdown, List <Measure>> b2 : breakdownDetector.list) {
				subspaceAndBreakDown.add(Tuple4.of(new ArrayList <>(), b2.f0, b2.f1, 1.0));
			}

			end = System.currentTimeMillis();
			System.out.println("whole table detect run time: " + (end - start) / 1000 + "s.");
		}

		// for 1-subspace.
		{
			long start = System.currentTimeMillis();

			ImpactDetector impactDetector = new ImpactDetector(impactThreshold);
			impactDetector.detect(data, breakdownCols, threadNum);

			long end = System.currentTimeMillis();
			System.out.println("subspace impact detect run time: " + (end - start) / 1000 + "s.");
			start = System.currentTimeMillis();

			for (Tuple2 <Subspace, Double> t2 : impactDetector.listSingleSubspace()) {
				BreakdownDetector breakdownDetector = new BreakdownDetector().detect(data,
					Collections.singletonList(t2.f0), false, breakDownThreshold, threadNum);
				for (Tuple2 <Breakdown, List <Measure>> b2 : breakdownDetector.list) {
					subspaceAndBreakDown.add(Tuple4.of(Collections.singletonList(t2.f0), b2.f0, b2.f1, t2.f1));
				}
			}
			end = System.currentTimeMillis();
			System.out.println("subspace breakdown detect run time: " + (end - start) / 1000 + "s.");

			start = System.currentTimeMillis();
			subspaceAndBreakDownByCol = impactDetector.listSubspaceByCol();

			end = System.currentTimeMillis();
			System.out.println("subspace cross detect run time: " + (end - start) / 1000 + "s.");

		}
		return Tuple2.of(subspaceAndBreakDown, subspaceAndBreakDownByCol);
	}

}
